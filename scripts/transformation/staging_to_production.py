import pandas as pd
import json
import os
import yaml
from datetime import datetime
from sqlalchemy import create_engine, text

# -------------------------------------------------
# LOAD CONFIG
# -------------------------------------------------
with open("config/config.yaml", "r") as f:
    config = yaml.safe_load(f)

db_cfg = config["database"]

DB_HOST = os.getenv("DB_HOST", db_cfg["host"].replace("${DB_HOST}", "localhost"))
DB_PORT = db_cfg["port"]
DB_NAME = db_cfg["name"]
DB_USER = os.getenv("DB_USER", db_cfg["user"].replace("${DB_USER}", "admin"))
DB_PASSWORD = os.getenv("DB_PASSWORD", db_cfg["password"].replace("${DB_PASSWORD}", "password"))

ENGINE_URL = (
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}"
    f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

engine = create_engine(ENGINE_URL)

OUTPUT_PATH = "data/processed"
os.makedirs(OUTPUT_PATH, exist_ok=True)

# -------------------------------------------------
# CLEANSE FUNCTIONS
# -------------------------------------------------
def cleanse_customer_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["first_name"] = df["first_name"].str.strip().str.title()
    df["last_name"] = df["last_name"].str.strip().str.title()
    df["email"] = df["email"].str.strip().str.lower()
    df["phone"] = df["phone"].astype(str).str.replace(r"\D", "", regex=True)
    return df


def cleanse_product_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["profit_margin"] = ((df["price"] - df["cost"]) / df["price"]) * 100

    df["price_category"] = pd.cut(
        df["price"],
        bins=[0, 50, 200, 100000],
        labels=["Budget", "Mid-range", "Premium"],
        include_lowest=True
    ).astype(str)  # CRITICAL FIX

    return df


def apply_business_rules(df: pd.DataFrame, rule_type: str) -> pd.DataFrame:
    df = df.copy()
    if rule_type == "transactions":
        df = df[df["total_amount"] > 0]
    if rule_type == "transaction_items":
        df = df[df["quantity"] > 0]
    return df


# -------------------------------------------------
# LOAD TO PRODUCTION (STRICT SCHEMA ALIGNMENT)
# -------------------------------------------------
def load_to_production(df: pd.DataFrame, table_name: str, connection) -> dict:
    df = df.copy()

    if table_name == "customers":
        df = df[
            [
                "customer_id",
                "first_name",
                "last_name",
                "email",
                "phone",
                "registration_date",
                "city",
                "state",
                "country",
                "age_group",
            ]
        ]

    elif table_name == "products":
        df = df[
            [
                "product_id",
                "product_name",
                "category",
                "sub_category",
                "price",
                "cost",
                "brand",
                "stock_quantity",
                "supplier_id",
                "profit_margin",
                "price_category",
            ]
        ]

    elif table_name == "transactions":
        df = df[
            [
                "transaction_id",
                "customer_id",
                "transaction_date",
                "transaction_time",
                "payment_method",
                "shipping_address",
                "total_amount",
            ]
        ]

    elif table_name == "transaction_items":
        df = df[
            [
                "item_id",
                "transaction_id",
                "product_id",
                "quantity",
                "unit_price",
                "discount_percentage",
                "line_total",
            ]
        ]

    df.to_sql(
        table_name,
        connection,
        schema="production",
        if_exists="append",
        index=False,
        method=None  # 🔥 disables multi-bind chaos
    )

    return {"rows_loaded": len(df)}


# -------------------------------------------------
# FK SAFE TRUNCATION
# -------------------------------------------------
def truncate_production_tables(connection):
    connection.execute(text("""
        TRUNCATE TABLE
            production.transaction_items,
            production.transactions,
            production.products,
            production.customers
        CASCADE
    """))

def enforce_product_quality(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["product_name"] = df["product_name"].fillna("Unknown Product")
    df["category"] = df["category"].fillna("Unknown")
    df["sub_category"] = df["sub_category"].fillna("Unknown")
    df["brand"] = df["brand"].fillna("Unknown Brand")

    df = df[df["price"] > 0]
    df = df[df["cost"] >= 0]

    return df


# -------------------------------------------------
# MAIN
# -------------------------------------------------
if __name__ == "__main__":

    summary = {
        "transformation_timestamp": datetime.now().isoformat(),
        "records_processed": {},
        "transformations_applied": [
            "text_normalization",
            "email_standardization",
            "phone_standardization",
            "profit_margin_calculation",
            "price_categorization",
            "business_rule_filtering",
            "schema_alignment_enforced"
        ]
    }

    with engine.begin() as conn:

        # READ FROM STAGING (NO loaded_at SELECTED)
        customers_df = pd.read_sql("""
            SELECT customer_id, first_name, last_name, email, phone,
                   registration_date, city, state, country, age_group
            FROM staging.customers
        """, conn)

        products_df = pd.read_sql("""
            SELECT product_id, product_name, category, sub_category,
                   price, cost, brand, stock_quantity, supplier_id
            FROM staging.products
        """, conn)

        transactions_df = pd.read_sql("""
            SELECT transaction_id, customer_id, transaction_date,
                   transaction_time, payment_method, shipping_address,
                   total_amount
            FROM staging.transactions
        """, conn)

        items_df = pd.read_sql("""
            SELECT item_id, transaction_id, product_id,
                   quantity, unit_price, discount_percentage, line_total
            FROM staging.transaction_items
        """, conn)

        # TRANSFORM
        customers_df = cleanse_customer_data(customers_df)
        products_df = cleanse_product_data(products_df)
        products_df = enforce_product_quality(products_df)
        transactions_df = apply_business_rules(transactions_df, "transactions")
        items_df = apply_business_rules(items_df, "transaction_items")
        


        # TRUNCATE + LOAD
        truncate_production_tables(conn)

        summary["records_processed"]["customers"] = load_to_production(customers_df, "customers", conn)
        summary["records_processed"]["products"] = load_to_production(products_df, "products", conn)
        summary["records_processed"]["transactions"] = load_to_production(transactions_df, "transactions", conn)
        summary["records_processed"]["transaction_items"] = load_to_production(items_df, "transaction_items", conn)

    with open(os.path.join(OUTPUT_PATH, "transformation_summary.json"), "w") as f:
        json.dump(summary, f, indent=4)

    print("✅ Staging → Production ETL completed successfully")
