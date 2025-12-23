import pandas as pd
import numpy as np
import random
from faker import Faker
from datetime import datetime
import json
import os
import yaml

with open("config/config.yaml", "r") as f:
    config = yaml.safe_load(f)

START_DATE = config["data_generation"]["start_date"]
END_DATE = config["data_generation"]["end_date"]
CUSTOMER_COUNT = config["data_generation"]["customers"]
PRODUCT_COUNT = config["data_generation"]["products"]
TRANSACTION_COUNT = config["data_generation"]["transactions"]


fake = Faker()


def generate_customers(num_customers: int) -> pd.DataFrame:
    customers = []
    emails = set()

    for i in range(1, num_customers + 1):
        email = fake.email()
        while email in emails:
            email = fake.email()
        emails.add(email)

        customers.append({
            "customer_id": f"CUST{i:04d}",
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": email,
            "phone": fake.phone_number(),
            "registration_date": fake.date_between(start_date='-3y', end_date='today'),
            "city": fake.city(),
            "state": fake.state(),
            "country": fake.country(),
            "age_group": random.choice(["18-25", "26-35", "36-45", "46-60", "60+"])
        })

    return pd.DataFrame(customers)



def generate_products(num_products: int) -> pd.DataFrame:
    categories = {
        "Electronics": ["Mobile", "Laptop", "Headphones"],
        "Clothing": ["Shirts", "Jeans", "Jackets"],
        "Home & Kitchen": ["Cookware", "Furniture"],
        "Books": ["Fiction", "Education"],
        "Sports": ["Fitness", "Outdoor"],
        "Beauty": ["Skincare", "Makeup"]
    }

    products = []

    for i in range(1, num_products + 1):
        category = random.choice(list(categories.keys()))
        price = round(random.uniform(200, 50000), 2)
        cost = round(price * random.uniform(0.6, 0.9), 2)

        products.append({
            "product_id": f"PROD{i:04d}",
            "product_name": fake.word().capitalize(),
            "category": category,
            "sub_category": random.choice(categories[category]),
            "price": price,
            "cost": cost,
            "brand": fake.company(),
            "stock_quantity": random.randint(10, 500),
            "supplier_id": f"SUPP{random.randint(1,50):03d}"
        })

    return pd.DataFrame(products)


def generate_transactions(num_transactions: int, customers_df: pd.DataFrame) -> pd.DataFrame:
    transactions = []

    for i in range(1, num_transactions + 1):
        customer_id = random.choice(customers_df["customer_id"].tolist())
        txn_date = fake.date_between(start_date=START_DATE, end_date=END_DATE)

        transactions.append({
            "transaction_id": f"TXN{i:05d}",
            "customer_id": customer_id,
            "transaction_date": txn_date,
            "transaction_time": fake.time(),
            "payment_method": random.choice(
                ["Credit Card", "Debit Card", "UPI", "Cash on Delivery", "Net Banking"]
            ),
            "shipping_address": fake.address().replace("\n", ", "),
            "total_amount": 0.0
        })

    return pd.DataFrame(transactions)



def generate_transaction_items(transactions_df: pd.DataFrame, products_df: pd.DataFrame) -> pd.DataFrame:
    items = []
    item_id = 1

    for _, txn in transactions_df.iterrows():
        num_items = random.randint(1, 5)

        for _ in range(num_items):
            product = products_df.sample(1).iloc[0]
            quantity = random.randint(1, 5)
            discount = random.choice([0, 5, 10, 15, 20, 25, 30])

            line_total = round(
                quantity * product["price"] * (1 - discount / 100), 2
            )

            items.append({
                "item_id": f"ITEM{item_id:05d}",
                "transaction_id": txn["transaction_id"],
                "product_id": product["product_id"],
                "quantity": quantity,
                "unit_price": product["price"],
                "discount_percentage": discount,
                "line_total": line_total
            })

            item_id += 1

    return pd.DataFrame(items)


def validate_referential_integrity(customers, products, transactions, items) -> dict:
    orphan_txns = transactions[
        ~transactions["customer_id"].isin(customers["customer_id"])
    ]

    orphan_items_txn = items[
        ~items["transaction_id"].isin(transactions["transaction_id"])
    ]

    orphan_items_prod = items[
        ~items["product_id"].isin(products["product_id"])
    ]

    violations = len(orphan_txns) + len(orphan_items_txn) + len(orphan_items_prod)

    return {
        "orphan_transactions": int(len(orphan_txns)),
        "orphan_transaction_items_txn": int(len(orphan_items_txn)),
        "orphan_transaction_items_prod": int(len(orphan_items_prod)),
        "violations": int(violations),
        "data_quality_score": 100 if violations == 0 else 90
    }


if __name__ == "__main__":
    os.makedirs("data/raw", exist_ok=True)

    customers = generate_customers(CUSTOMER_COUNT)
    products = generate_products(PRODUCT_COUNT)
    transactions = generate_transactions(TRANSACTION_COUNT, customers)
    items = generate_transaction_items(transactions, products)

    # Calculate total_amount
    totals = items.groupby("transaction_id")["line_total"].sum().reset_index()
    transactions = transactions.merge(totals, on="transaction_id", suffixes=("", "_sum"))
    transactions["total_amount"] = transactions["line_total"]
    transactions.drop(columns=["line_total"], inplace=True)

    # Save CSVs
    customers.to_csv("data/raw/customers.csv", index=False)
    products.to_csv("data/raw/products.csv", index=False)
    transactions.to_csv("data/raw/transactions.csv", index=False)
    items.to_csv("data/raw/transaction_items.csv", index=False)

    metadata = {
        "generation_timestamp": datetime.utcnow().isoformat(),
        "record_counts": {
            "customers": len(customers),
            "products": len(products),
            "transactions": len(transactions),
            "transaction_items": len(items)
        }
    }

    with open("data/raw/generation_metadata.json", "w") as f:
        json.dump(metadata, f, indent=4)

    print("Data Generation Completed")
