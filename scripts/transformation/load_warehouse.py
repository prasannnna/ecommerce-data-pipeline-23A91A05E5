import os
import yaml
import pandas as pd
from sqlalchemy import create_engine, text

with open("config/config.yaml", "r") as f:
    config = yaml.safe_load(f)

db = config["database"]

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = db.get("port", 5432)
DB_NAME = db.get("name", "ecommerce_db")
DB_USER = os.getenv("DB_USER", "admin")
DB_PASSWORD = os.getenv("DB_PASSWORD", "password")

ENGINE_URL = (
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}"
    f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

engine = create_engine(ENGINE_URL)


def truncate_warehouse_tables(conn):
    conn.execute(text("""
        TRUNCATE TABLE
            warehouse.fact_sales,
            warehouse.agg_daily_sales,
            warehouse.agg_product_performance,
            warehouse.agg_customer_metrics,
            warehouse.dim_customers,
            warehouse.dim_products,
            warehouse.dim_payment_method,
            warehouse.dim_date
    """))
    print(" Warehouse tables truncated safely")


def load_dim_date(conn):
    result = conn.execute(text("""
        SELECT 
            MIN(transaction_date) AS min_date,
            MAX(transaction_date) AS max_date
        FROM production.transactions
    """)).fetchone()

    start_date = result.min_date
    end_date = result.max_date

    dates = pd.date_range(start_date, end_date, freq="D")

    df = pd.DataFrame({"full_date": dates})
    df["date_key"] = df["full_date"].dt.strftime("%Y%m%d").astype(int)
    df["year"] = df["full_date"].dt.year
    df["quarter"] = df["full_date"].dt.quarter
    df["month"] = df["full_date"].dt.month
    df["day"] = df["full_date"].dt.day
    df["month_name"] = df["full_date"].dt.month_name()
    df["day_name"] = df["full_date"].dt.day_name()
    df["week_of_year"] = df["full_date"].dt.isocalendar().week.astype(int)
    df["is_weekend"] = df["day_name"].isin(["Saturday", "Sunday"])
    df["is_holiday"] = False

    df.to_sql(
        "dim_date",
        conn,
        schema="warehouse",
        if_exists="append",
        index=False,
        method="multi"
    )

    print(f"dim_date loaded: {len(df)} rows ({start_date} → {end_date})")


def load_dim_payment_method(conn):
    conn.execute(text("""
        INSERT INTO warehouse.dim_payment_method (payment_method_name, payment_type)
        SELECT DISTINCT
            payment_method,
            CASE
                WHEN payment_method = 'Cash on Delivery' THEN 'Offline'
                ELSE 'Online'
            END
        FROM production.transactions
        ON CONFLICT (payment_method_name) DO NOTHING;
    """))
    print("dim_payment_method loaded")


def load_dim_customers(conn):
    conn.execute(text("""
        INSERT INTO warehouse.dim_customers
        (customer_id, full_name, email, city, state, country, age_group,
         registration_date, effective_date, end_date, is_current)
        SELECT
            customer_id,
            first_name || ' ' || last_name,
            email,
            city,
            state,
            country,
            age_group,
            registration_date,
            CURRENT_DATE,
            NULL,
            TRUE
        FROM production.customers;
    """))
    print("dim_customers loaded")


def load_dim_products(conn):
    conn.execute(text("""
        INSERT INTO warehouse.dim_products
        (product_id, product_name, category, sub_category, brand,
         price_range, effective_date, end_date, is_current)
        SELECT
            product_id,
            product_name,
            category,
            sub_category,
            brand,
            price_category,
            CURRENT_DATE,
            NULL,
            TRUE
        FROM production.products;
    """))
    print("dim_products loaded")


def load_fact_sales(conn):
    conn.execute(text("""
        INSERT INTO warehouse.fact_sales
        (date_key, customer_key, product_key, payment_method_key,
         transaction_id, quantity, unit_price, discount_amount,
         line_total, profit, created_at)
        SELECT
            dd.date_key,
            dc.customer_key,
            dp.product_key,
            dpm.payment_method_key,
            ti.transaction_id,
            ti.quantity,
            ti.unit_price,
            ti.unit_price * ti.quantity * (ti.discount_percentage / 100),
            ti.line_total,
            ti.line_total - (p.cost * ti.quantity),
            CURRENT_TIMESTAMP
        FROM production.transaction_items ti
        JOIN production.transactions t ON ti.transaction_id = t.transaction_id
        JOIN production.products p ON ti.product_id = p.product_id
        JOIN warehouse.dim_date dd
        ON dd.date_key = CAST(TO_CHAR(t.transaction_date, 'YYYYMMDD') AS INTEGER)
        JOIN warehouse.dim_customers dc ON dc.customer_id = t.customer_id AND dc.is_current = TRUE
        JOIN warehouse.dim_products dp ON dp.product_id = ti.product_id AND dp.is_current = TRUE
        JOIN warehouse.dim_payment_method dpm ON dpm.payment_method_name = t.payment_method;
    """))
    print(" fact_sales loaded")


def load_aggregates(conn):
    conn.execute(text("""
        INSERT INTO warehouse.agg_daily_sales
        SELECT
            date_key,
            COUNT(DISTINCT transaction_id),
            SUM(line_total),
            SUM(profit),
            COUNT(DISTINCT customer_key)
        FROM warehouse.fact_sales
        GROUP BY date_key;
    """))
    print("aggregate tables loaded")


if __name__ == "__main__":
    with engine.begin() as conn:
        truncate_warehouse_tables(conn)

        load_dim_date(conn)
        load_dim_payment_method(conn)
        load_dim_customers(conn)
        load_dim_products(conn)
        load_fact_sales(conn)
        load_aggregates(conn)

    print("\n PHASE 3.3 WAREHOUSE LOAD COMPLETED SUCCESSFULLY")
