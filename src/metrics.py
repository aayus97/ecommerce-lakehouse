from pathlib import Path
from datetime import datetime, timezone
import json
import os

from src.config import load_app_config, path_value

config = load_app_config()

METRICS_DIR = Path(path_value(config, "metrics"))


def write_metric(metric_name: str, data: dict):
    METRICS_DIR.mkdir(exist_ok=True)

    record = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "metric_name": metric_name,
        "pipeline_name": os.getenv("PIPELINE_NAME"),
        "run_id": os.getenv("PIPELINE_RUN_ID"),
        **data,
    }

    metric_file = METRICS_DIR / f"{metric_name}.jsonl"

    with metric_file.open("a") as f:
        f.write(json.dumps(record) + "\n")
