from datetime import datetime, timezone
import json
import os
from pathlib import Path

from src.config import load_app_config, path_value

config = load_app_config()

METRICS_DIR = Path(path_value(config, "metrics"))


def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


def write_metric(metric_name: str, data: dict):
    METRICS_DIR.mkdir(parents=True, exist_ok=True)

    record = {
        "timestamp": utc_now_iso(),
        "metric_name": metric_name,
        "pipeline_name": os.getenv("PIPELINE_NAME"),
        "run_id": os.getenv("PIPELINE_RUN_ID"),
        **data,
    }

    metric_file = METRICS_DIR / f"{metric_name}.jsonl"

    with metric_file.open("a") as f:
        f.write(json.dumps(record) + "\n")


def write_step_metric(
    step: str,
    *,
    rows_read: int | None = None,
    rows_written: int | None = None,
    rows_quarantined: int | None = None,
    duration_seconds: float | None = None,
    input_path=None,
    output_path=None,
    status: str = "success",
    details: dict | None = None,
):
    payload = {
        "step": step,
        "status": status,
        "rows_read": rows_read,
        "rows_written": rows_written,
        "rows_quarantined": rows_quarantined,
        "duration_seconds": duration_seconds,
        "input_path": input_path,
        "output_path": output_path,
    }

    if details:
        payload.update(details)

    write_metric("step_metrics", payload)
