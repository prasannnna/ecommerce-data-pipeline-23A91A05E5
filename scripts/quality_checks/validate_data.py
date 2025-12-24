import json
import yaml
from datetime import datetime
from sqlalchemy import create_engine, text
import os

# ------------------------------
# LOAD CONFIG
# ------------------------------
with open("config/config.yaml", "r") as f:
    config = yaml.safe_load(f)

db = config["database"]

engine = create_engine(
    f"postgresql+psycopg2://{db['user'].replace('${DB_USER}','admin')}:"
    f"{db['password'].replace('${DB_PASSWORD}','password')}@"
    f"{db['host'].replace('${DB_HOST}','localhost')}:{db['port']}/{db['name']}"
)

SQL_FILE = "sql/queries/data_quality_checks.sql"
OUTPUT_DIR = "data/staging"

os.makedirs(OUTPUT_DIR, exist_ok=True)


def run_scalar(query):
    with engine.connect() as conn:
        return conn.execute(text(query)).scalar()


def calculate_score(violations, total, weight):
    if total == 0:
        return weight
    score = max(0, (1 - violations / total)) * weight
    return round(score, 2)


if __name__ == "__main__":

    total_records = {
        "customers": run_scalar("SELECT COUNT(*) FROM production.customers"),
        "products": run_scalar("SELECT COUNT(*) FROM production.products"),
        "transactions": run_scalar("SELECT COUNT(*) FROM production.transactions"),
        "items": run_scalar("SELECT COUNT(*) FROM production.transaction_items"),
    }

    checks = {}

    # ---------------- COMPLETENESS ----------------
    null_products = run_scalar("""
        SELECT COUNT(*) FROM production.products
        WHERE product_name IS NULL OR category IS NULL OR price IS NULL
    """)

    checks["null_checks"] = {
        "status": "passed" if null_products == 0 else "failed",
        "null_violations": null_products,
        "details": {"production.products": null_products}
    }

    # ---------------- UNIQUENESS ----------------
    duplicate_emails = run_scalar("""
        SELECT COUNT(*) FROM (
            SELECT email FROM production.customers
            GROUP BY email HAVING COUNT(*) > 1
        ) x
    """)

    checks["duplicate_checks"] = {
        "status": "passed" if duplicate_emails == 0 else "failed",
        "duplicates_found": duplicate_emails,
        "details": {"duplicate_emails": duplicate_emails}
    }

    # ---------------- REFERENTIAL ----------------
    orphan_items = run_scalar("""
        SELECT COUNT(*) FROM production.transaction_items ti
        LEFT JOIN production.transactions t
        ON ti.transaction_id = t.transaction_id
        WHERE t.transaction_id IS NULL
    """)

    checks["referential_integrity"] = {
        "status": "passed" if orphan_items == 0 else "failed",
        "orphan_records": orphan_items,
        "details": {"transaction_items.transaction_id": orphan_items}
    }

    # ---------------- CONSISTENCY ----------------
    line_mismatch = run_scalar("""
        SELECT COUNT(*) FROM production.transaction_items
        WHERE ABS(line_total - (quantity * unit_price * (1 - discount_percentage/100))) > 0.01
    """)

    checks["data_consistency"] = {
        "status": "passed" if line_mismatch == 0 else "failed",
        "mismatches": line_mismatch,
        "details": {"line_total_mismatch": line_mismatch}
    }

    # ---------------- SCORING ----------------
    overall_score = (
        calculate_score(null_products, total_records["products"], 30) +
        calculate_score(duplicate_emails, total_records["customers"], 20) +
        calculate_score(orphan_items, total_records["items"], 30) +
        calculate_score(line_mismatch, total_records["items"], 20)
    )

    grade = (
        "A" if overall_score >= 90 else
        "B" if overall_score >= 80 else
        "C" if overall_score >= 70 else
        "D" if overall_score >= 60 else
        "F"
    )

    report = {
        "check_timestamp": datetime.now().isoformat(),
        "checks_performed": checks,
        "overall_quality_score": overall_score,
        "quality_grade": grade
    }

    with open(f"{OUTPUT_DIR}/quality_report.json", "w") as f:
        json.dump(report, f, indent=4)

    print("✅ Data Quality Validation Completed")
    print(f"Overall Score: {overall_score} | Grade: {grade}")
