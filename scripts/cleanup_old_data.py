import os
import time
import yaml
from pathlib import Path

with open("config/config.yaml") as f:
    config = yaml.safe_load(f)

RETENTION_DAYS = config["pipeline"].get("retention_days", 7)
NOW = time.time()

TARGET_DIRS = ["data/raw", "data/staging", "logs"]

for directory in TARGET_DIRS:
    for file in Path(directory).glob("*"):
        if "summary" in file.name or "report" in file.name:
            continue

        if file.is_file():
            age_days = (NOW - file.stat().st_mtime) / 86400
            if age_days > RETENTION_DAYS:
                file.unlink()
