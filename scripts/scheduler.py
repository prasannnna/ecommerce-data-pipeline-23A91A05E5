import schedule
import subprocess
import time
import logging
from pathlib import Path
from datetime import datetime
import yaml

# ---------------- LOAD CONFIG ----------------
with open("config/config.yaml") as f:
    config = yaml.safe_load(f)

RUN_TIME = config["pipeline"].get("schedule_time", "02:00")

# ---------------- LOGGING ----------------
LOG_FILE = Path("logs/scheduler_activity.log")
LOG_FILE.parent.mkdir(exist_ok=True)

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

LOCK_FILE = Path("logs/pipeline.lock")

# ---------------- PIPELINE RUN ----------------
def run_pipeline():
    if LOCK_FILE.exists():
        logging.warning("Pipeline already running. Skipping execution.")
        return

    try:
        LOCK_FILE.touch()
        logging.info("Starting scheduled pipeline run")

        subprocess.run(
            ["python", "scripts/pipeline_orchestrator.py"],
            check=True
        )

        subprocess.run(
            ["python", "scripts/cleanup_old_data.py"],
            check=True
        )

        logging.info("Pipeline completed successfully")

    except Exception as e:
        logging.error(f"Pipeline execution failed: {str(e)}")

    finally:
        LOCK_FILE.unlink(missing_ok=True)

# ---------------- SCHEDULER ----------------

schedule.every().day.at(RUN_TIME).do(run_pipeline)

# schedule.every(1).minutes.do(run_pipeline)

logging.info("Scheduler started")

while True:
    schedule.run_pending()
    time.sleep(30)
