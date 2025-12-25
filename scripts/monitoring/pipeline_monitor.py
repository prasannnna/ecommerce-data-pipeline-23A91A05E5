import os
import time
import json
from datetime import datetime
from pathlib import Path

import yaml
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# -------------------------------------------------
# Load environment variables
# -------------------------------------------------
load_dotenv()

# -------------------------------------------------
# Load config.yaml (fallbacks only, NOT credentials)
# -------------------------------------------------
with open("config/config.yaml") as f:
    config = yaml.safe_load(f)

# -------------------------------------------------
# Database configuration (ENV first, YAML fallback)
# -------------------------------------------------
DB_HOST = os.getenv("DB_HOST", config["database"]["host"])
DB_PORT = os.getenv("DB_PORT", config["database"]["port"])
DB_NAME = os.getenv("DB_NAME", config["database"]["name"])
DB_USER = os.getenv("DB_USER", config["database"]["user"])
DB_PASSWORD = os.getenv("DB_PASSWORD", config["database"]["password"])

if not all([DB_HOST, DB_NAME, DB_USER, DB_PASSWORD]):
    raise RuntimeError("Database environment variables are not properly set")

engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}",
    pool_pre_ping=True
)

# -------------------------------------------------
# Monitoring Logic
# -------------------------------------------------
def monitor():
    start_time = time.time()

    report = {
        "monitoring_timestamp": datetime.now().isoformat(),
        "pipeline_health": "healthy",
        "checks": {},
        "alerts": [],
        "overall_health_score": 100
    }

    with engine.connect() as conn:
        # -----------------------------
        # Data volume check
        # -----------------------------
        sales_count = conn.execute(
            text("SELECT COUNT(*) FROM warehouse.fact_sales")
        ).scalar()

        report["checks"]["data_volume"] = {
            "status": "ok" if sales_count > 0 else "critical",
            "actual_count": sales_count
        }

        if sales_count == 0:
            report["pipeline_health"] = "critical"
            report["overall_health_score"] = 60
            report["alerts"].append({
                "severity": "critical",
                "check": "data_volume",
                "message": "No records found in warehouse.fact_sales",
                "timestamp": datetime.now().isoformat()
            })

        # -----------------------------
        # Data freshness check
        # -----------------------------
        latest_fact = conn.execute(
            text("SELECT MAX(created_at) FROM warehouse.fact_sales")
        ).scalar()

        report["checks"]["data_freshness"] = {
            "status": "ok" if latest_fact else "warning",
            "warehouse_latest_record": str(latest_fact)
        }

    # -------------------------------------------------
    # Save monitoring report
    # -------------------------------------------------
    output_path = Path("data/processed/monitoring_report.json")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w") as f:
        json.dump(report, f, indent=4)

    duration = round(time.time() - start_time, 2)
    print(f" Monitoring completed in {duration}s")

# -------------------------------------------------
# Entry point
# -------------------------------------------------
if __name__ == "__main__":
    monitor()
