import os
import pandas as pd

def test_generated_files_exist():
    assert os.path.exists("data/raw/customers.csv")
    assert os.path.exists("data/raw/products.csv")
    assert os.path.exists("data/raw/transactions.csv")
    assert os.path.exists("data/raw/transaction_items.csv")

def test_referential_integrity():
    customers = pd.read_csv("data/raw/customers.csv")
    transactions = pd.read_csv("data/raw/transactions.csv")

    assert transactions["customer_id"].isin(customers["customer_id"]).all()
