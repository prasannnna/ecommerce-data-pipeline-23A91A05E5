import json
import os

def test_quality_report_exists():
    assert os.path.exists("data/staging/quality_report.json")

def test_quality_score_present():
    with open("data/staging/quality_report.json") as f:
        report = json.load(f)
    assert "overall_quality_score" in report
