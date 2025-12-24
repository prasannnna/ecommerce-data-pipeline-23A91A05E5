import pandas as pd
import json
import time
import os
import yaml
from datetime import datetime
from sqlalchemy import create_engine, text


with open("config/config.yaml", "r") as f:
    config = yaml.safe_load(f)

db_cfg = config["database"]

DB_HOST = os.getenv(
    "DB_HOST",
    db_cfg["host"].replace("${DB_HOST}", "localhost")
)
DB_PORT = os.getenv(
    "DB_PORT",
    str(db_cfg["port"])
)
DB_NAME = os.getenv(
    "DB_NAME",
    db_cfg["name"]
)
DB_USER = os.getenv(
    "DB_USER",
    db_cfg["user"].replace("${DB_USER}", "admin")
)
DB_PASSWORD = os.getenv(
    "DB_PASSWORD",
    db_cfg["password"].replace("${DB_PASSWORD}", "password")
)

ENGINE_URL = (
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}"
    f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)


PIPELINE_CFG = config.get("pipeline", {})
BATCH_SIZE = PIPELINE_CFG.get("batch_size", 1000)

RAW_DATA_PATH = "data/raw"
OUTPUT_PATH = "data/staging"
os.makedirs(OUTPUT_PATH, exist_ok=True)

TABLE_ORDER = [
    "customers",
    "products",
    "transactions",
    "transaction_items"
]


def load_csv_to_staging(csv_path: str, table_name: str, connection) -> dict:
    df = pd.read_csv(csv_path)

    df.to_sql(
        table_name,
        connection,
        schema="staging",
        if_exists="append",
        index=False,
        method="multi",
        chunksize=BATCH_SIZE
    )

    return {
        "rows_loaded": len(df),
        "status": "success"
    }


def bulk_insert_data(df: pd.DataFrame, table_name: str, connection) -> int:
    df.to_sql(
        table_name,
        connection,
        schema="staging",
        if_exists="append",
        index=False,
        method="multi",
        chunksize=BATCH_SIZE
    )
    return len(df)


def validate_staging_load(connection) -> dict:
    results = {}
    for table in TABLE_ORDER:
        count = connection.execute(
            text(f"SELECT COUNT(*) FROM staging.{table}")
        ).scalar()
        results[table] = count
    return results


if __name__ == "__main__":
    start_time = time.time()
    engine = create_engine(ENGINE_URL)

    ingestion_report = {
        "ingestion_timestamp": datetime.now().isoformat(),
        "tables_loaded": {},
        "row_counts": {},
        "total_execution_time_seconds": None
    }

    try:
       
        with engine.begin() as conn:
            # Truncate tables (reverse order for safety)
            for table in reversed(TABLE_ORDER):
                conn.execute(text(f"TRUNCATE TABLE staging.{table}"))

            # Load CSV files
            for table in TABLE_ORDER:
                csv_file = os.path.join(RAW_DATA_PATH, f"{table}.csv")

                if not os.path.exists(csv_file):
                    raise FileNotFoundError(f"Missing CSV file: {csv_file}")

                result = load_csv_to_staging(csv_file, table, conn)
                ingestion_report["tables_loaded"][f"staging.{table}"] = result

       
        with engine.connect() as conn:
            ingestion_report["row_counts"] = validate_staging_load(conn)

        ingestion_report["total_execution_time_seconds"] = round(
            time.time() - start_time, 2
        )

        with open(
            os.path.join(OUTPUT_PATH, "ingestion_summary.json"),
            "w"
        ) as f:
            json.dump(ingestion_report, f, indent=4)

        print("Data ingestion into staging completed successfully")

    except Exception as e:
        ingestion_report["error"] = str(e)

        with open(
            os.path.join(OUTPUT_PATH, "ingestion_summary.json"),
            "w"
        ) as f:
            json.dump(ingestion_report, f, indent=4)

        raise
