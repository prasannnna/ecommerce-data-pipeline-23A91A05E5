import pandas as pd
import random
import json
import yaml
from faker import Faker
from datetime import datetime, date
import os


fake = Faker()
random.seed(42)
Faker.seed(42)

# Ensure output directories exist
os.makedirs("data/raw", exist_ok=True)


with open("config/config.yaml", "r") as f:
    config = yaml.safe_load(f)

cfg = config["data_generation"]


START_DATE = datetime.strptime(cfg["start_date"], "%Y-%m-%d").date()
END_DATE = datetime.strptime(cfg["end_date"], "%Y-%m-%d").date()


def generate_customers(num_customers: int) -> pd.DataFrame:
    customers = []

    for i in range(1, num_customers + 1):
        customers.append({
            "customer_id": f"CUST{i:04d}",
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": fake.unique.email(),
            "phone": fake.phone_number(),
            "registration_date": fake.date_between(START_DATE, END_DATE),
            "city": fake.city(),
            "state": fake.state(),
            "country": "India",
            "age_group": random.choice(["18-25", "26-35", "36-45", "46+"])
        })

    return pd.DataFrame(customers)


def generate_products(num_products: int) -> pd.DataFrame:
    categories = ["Electronics", "Clothing", "Books", "Sports", "Beauty", "Home & Kitchen"]
    products = []

    for i in range(1, num_products + 1):
        price = round(random.uniform(200, 50000), 2)
        cost = round(price * random.uniform(0.6, 0.9), 2)

        products.append({
            "product_id": f"PROD{i:04d}",
            "product_name": fake.word().capitalize(),
            "category": random.choice(categories),
            "sub_category": fake.word(),
            "price": price,
            "cost": cost,
            "brand": fake.company(),
            "stock_quantity": random.randint(10, 500),
            "supplier_id": f"SUP{random.randint(1, 50):03d}"
        })

    return pd.DataFrame(products)


def generate_transactions(num_transactions: int, customers_df: pd.DataFrame) -> pd.DataFrame:
    transactions = []

    for i in range(1, num_transactions + 1):
        transactions.append({
            "transaction_id": f"TXN{i:05d}",
            "customer_id": random.choice(customers_df["customer_id"].tolist()),
            "transaction_date": fake.date_between(START_DATE, END_DATE),
            "transaction_time": fake.time(),
            "payment_method": random.choice([
                "Credit Card", "Debit Card", "UPI", "Cash on Delivery", "Net Banking"
            ]),
            "shipping_address": fake.address().replace("\n", ", "),
            "total_amount": 0.00  # calculated later
        })

    return pd.DataFrame(transactions)


def generate_transaction_items(transactions_df: pd.DataFrame,
                               products_df: pd.DataFrame) -> pd.DataFrame:
    items = []
    item_counter = 1
    transaction_totals = {}

    for _, txn in transactions_df.iterrows():
        txn_id = txn["transaction_id"]
        num_items = random.randint(1, 5)
        txn_total = 0.0

        for _ in range(num_items):
            product = products_df.sample(1).iloc[0]
            quantity = random.randint(1, 4)
            discount = random.choice([0, 5, 10, 15])

            line_total = round(
                quantity * product["price"] * (1 - discount / 100), 2
            )

            items.append({
                "item_id": f"ITEM{item_counter:05d}",
                "transaction_id": txn_id,
                "product_id": product["product_id"],
                "quantity": quantity,
                "unit_price": product["price"],
                "discount_percentage": discount,
                "line_total": line_total
            })

            txn_total += line_total
            item_counter += 1

        transaction_totals[txn_id] = round(txn_total, 2)

    
    transactions_df["total_amount"] = transactions_df["transaction_id"].map(transaction_totals)

    return pd.DataFrame(items)


def validate_referential_integrity(customers, products, transactions, items) -> dict:
    return {
        "customer_orphans": int(
            (~transactions["customer_id"].isin(customers["customer_id"])).sum()
        ),
        "product_orphans": int(
            (~items["product_id"].isin(products["product_id"])).sum()
        ),
        "transaction_orphans": int(
            (~items["transaction_id"].isin(transactions["transaction_id"])).sum()
        ),
        "quality_score": 100
    }


if __name__ == "__main__":
    print("Starting data generation...")

    customers_df = generate_customers(cfg["customers"])
    products_df = generate_products(cfg["products"])
    transactions_df = generate_transactions(cfg["transactions"], customers_df)
    items_df = generate_transaction_items(transactions_df, products_df)

    # Save CSVs
    customers_df.to_csv("data/raw/customers.csv", index=False)
    products_df.to_csv("data/raw/products.csv", index=False)
    transactions_df.to_csv("data/raw/transactions.csv", index=False)
    items_df.to_csv("data/raw/transaction_items.csv", index=False)

    # Metadata
    integrity_report = validate_referential_integrity(
        customers_df, products_df, transactions_df, items_df
    )

    metadata = {
        "generated_at": datetime.now().isoformat(),
        "date_range": {
            "start_date": cfg["start_date"],
            "end_date": cfg["end_date"]
        },
        "record_counts": {
            "customers": len(customers_df),
            "products": len(products_df),
            "transactions": len(transactions_df),
            "transaction_items": len(items_df)
        },
        "referential_integrity": integrity_report
    }

    with open("data/raw/generation_metadata.json", "w") as f:
        json.dump(metadata, f, indent=4)

    print("Data generation completed successfully")
