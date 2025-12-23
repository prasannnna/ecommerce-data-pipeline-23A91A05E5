import pandas as pd
import json
import time
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv
load_dotenv()


def load_csv_to_staging(csv_path: str, table_name: str, connection) -> dict:
    df = pd.read_csv(csv_path)
    rows = len(df)
    df.to_sql(table_name, connection, schema="staging", if_exists="append", index=False, method="multi")
    return {"rows_loaded": rows, "status": "success"}


def bulk_insert_data(df: pd.DataFrame, table_name: str, connection) -> int:
    df.to_sql(table_name, connection, schema="staging", if_exists="append", index=False, method="multi")
    return len(df)


def validate_staging_load(connection) -> dict:
    result = {}
    tables = ["customers", "products", "transactions", "transaction_items"]

    for table in tables:
        count = connection.execute(
            text(f"SELECT COUNT(*) FROM staging.{table}")
        ).scalar()
        result[table] = count

    return result


if __name__ == "__main__":
    start = time.time()

    engine = create_engine(
    f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    )
    conn = engine.connect()
    trans = conn.begin()

    summary = {}

    try:
        for table in ["customers", "products", "transactions", "transaction_items"]:
            conn.execute(text(f"TRUNCATE staging.{table}"))

            res = load_csv_to_staging(
                f"data/raw/{table}.csv",
                table,
                engine
            )

            summary[f"staging.{table}"] = res

        trans.commit()

    except Exception as e:
        trans.rollback()
        raise e

    execution_time = round(time.time() - start, 2)

    report = {
        "ingestion_timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "tables_loaded": summary,
        "total_execution_time_seconds": execution_time
    }

    os.makedirs("data/staging", exist_ok=True)
    with open("data/staging/ingestion_summary.json", "w") as f:
        json.dump(report, f, indent=4)

    print("Ingestion Completed")
