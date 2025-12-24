import os
import time
import json
import yaml
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
import re

# ---------------- LOAD CONFIG ----------------
with open("config/config.yaml", "r") as f:
    config = yaml.safe_load(f)

db = config["database"]

DB_HOST = os.getenv("DB_HOST", db["host"].replace("${DB_HOST}", "localhost"))
DB_PORT = os.getenv("DB_PORT", db["port"])
DB_NAME = os.getenv("DB_NAME", db["name"])
DB_USER = os.getenv("DB_USER", db["user"].replace("${DB_USER}", "admin"))
DB_PASSWORD = os.getenv("DB_PASSWORD", db["password"].replace("${DB_PASSWORD}", "password"))

ENGINE_URL = (
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}"
    f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

engine = create_engine(ENGINE_URL)

SQL_FILE = "sql/queries/analytical_queries.sql"
OUTPUT_DIR = "data/processed/analytics"
os.makedirs(OUTPUT_DIR, exist_ok=True)

CSV_NAMES = {
    1: "query1_top_products.csv",
    2: "query2_monthly_trend.csv",
    3: "query3_customer_segmentation.csv",
    4: "query4_category_performance.csv",
    5: "query5_payment_distribution.csv",
    6: "query6_geographic_analysis.csv",
    7: "query7_customer_lifetime_value.csv",
    8: "query8_product_profitability.csv",
    9: "query9_day_of_week_pattern.csv",
    10: "query10_discount_impact.csv",
}


def execute_query(connection, sql):
    start = time.time()
    df = pd.read_sql(text(sql), connection)
    elapsed_ms = round((time.time() - start) * 1000, 2)
    return df, elapsed_ms


def export_to_csv(df, filename):
    df.to_csv(os.path.join(OUTPUT_DIR, filename), index=False)


def generate_summary(results, total_time):
    return {
        "generation_timestamp": datetime.now().isoformat(),
        "queries_executed": len(results),
        "query_results": results,
        "total_execution_time_seconds": round(total_time, 2),
    }




def load_queries():
    with open(SQL_FILE, "r") as f:
        sql_text = f.read()

    # Split on -- Q1, -- Q2, -- Q10 ...
    parts = re.split(r"--\s*Q\d+.*", sql_text)

    queries = []
    for part in parts:
        sql = part.strip()

        if not sql:
            continue

        if re.search(r"\bselect\b", sql, re.IGNORECASE):
            queries.append(sql)

    return queries




if __name__ == "__main__":
    queries = load_queries()
    results = {}
    start_time = time.time()

    with engine.connect() as conn:
        for i, sql in enumerate(queries, start=1):
            print(f"Executing Query {i}")
            df, exec_time = execute_query(conn, sql)
            export_to_csv(df, CSV_NAMES[i])

            results[f"query{i}"] = {
                "rows": len(df),
                "columns": len(df.columns),
                "execution_time_ms": exec_time,
            }

    summary = generate_summary(results, time.time() - start_time)

    with open(os.path.join(OUTPUT_DIR, "analytics_summary.json"), "w") as f:
        json.dump(summary, f, indent=4)

    print("\nANALYTICS GENERATION COMPLETED SUCCESSFULLY")
