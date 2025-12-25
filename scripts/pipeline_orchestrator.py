import subprocess
import time
import json
import logging
from datetime import datetime
from pathlib import Path

# ---------------- CONFIG ----------------
PIPELINE_ID = f"PIPE_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

REPORT_PATH = Path("data/processed/pipeline_execution_report.json")

PIPELINE_STEPS = [
    ("data_generation", ["python", "scripts/data_generation/generate_data.py"]),
    ("ingestion", ["python", "scripts/ingestion/ingest_to_staging.py"]),
    ("quality_checks", ["python", "scripts/quality_checks/validate_data.py"]),
    ("staging_to_production", ["python", "scripts/transformation/staging_to_production.py"]),
    ("warehouse_load", ["python", "scripts/transformation/load_warehouse.py"]),
    ("analytics", ["python", "scripts/transformation/generate_analytics.py"]),
]

MAX_RETRIES = 3
BACKOFF_SECONDS = [1, 2, 4]

# ---------------- LOGGING ----------------
log_file = LOG_DIR / f"pipeline_orchestrator_{PIPELINE_ID}.log"
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

error_log = LOG_DIR / "pipeline_errors.log"

# ---------------- ORCHESTRATION ----------------
def run_step(step_name, command):
    for attempt in range(MAX_RETRIES):
        start = time.time()
        try:
            logging.info(f"STARTING STEP: {step_name}")
            subprocess.run(command, check=True)
            duration = round(time.time() - start, 2)
            logging.info(f"COMPLETED STEP: {step_name} in {duration}s")
            return {
                "status": "success",
                "duration_seconds": duration,
                "retry_attempts": attempt
            }
        except Exception as e:
            logging.error(f"FAILED STEP: {step_name} | Attempt {attempt+1}")
            with open(error_log, "a") as f:
                f.write(f"{datetime.now()} | {step_name} | {str(e)}\n")

            if attempt < MAX_RETRIES - 1:
                time.sleep(BACKOFF_SECONDS[attempt])
            else:
                return {
                    "status": "failed",
                    "duration_seconds": round(time.time() - start, 2),
                    "retry_attempts": MAX_RETRIES,
                    "error_message": str(e)
                }

# ---------------- MAIN ----------------
if __name__ == "__main__":
    start_time = datetime.now()
    report = {
        "pipeline_execution_id": PIPELINE_ID,
        "start_time": start_time.isoformat(),
        "status": "success",
        "steps_executed": {},
        "errors": [],
        "warnings": []
    }

    for step_name, command in PIPELINE_STEPS:
        result = run_step(step_name, command)
        report["steps_executed"][step_name] = result

        if result["status"] == "failed":
            report["status"] = "failed"
            report["errors"].append(f"{step_name} failed")
            break

    end_time = datetime.now()
    report["end_time"] = end_time.isoformat()
    report["total_duration_seconds"] = round((end_time - start_time).total_seconds(), 2)

    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(REPORT_PATH, "w") as f:
        json.dump(report, f, indent=4)

    logging.info(f"PIPELINE STATUS: {report['status']}")
