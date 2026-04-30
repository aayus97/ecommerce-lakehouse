# utils/metrics.py

from pathlib import Path
from datetime import datetime, timezone
import json
import time
import traceback

METRICS_DIR = Path("metrics")
METRICS_FILE = METRICS_DIR / "pipeline_metrics.jsonl"


def write_metric(
    job_name: str,
    status: str,
    rows_read: int = 0,
    rows_written: int = 0,
    bad_rows: int = 0,
    duration_seconds: float = 0.0,
    error_message: str | None = None,
):
    METRICS_DIR.mkdir(exist_ok=True)

    record = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "job_name": job_name,
        "status": status,
        "rows_read": rows_read,
        "rows_written": rows_written,
        "bad_rows": bad_rows,
        "duration_seconds": round(duration_seconds, 3),
        "error_message": error_message,
    }

    with METRICS_FILE.open("a") as f:
        f.write(json.dumps(record) + "\n")


def run_with_metrics(job_name: str, job_func):
    start = time.time()

    try:
        result = job_func()

        duration = time.time() - start

        write_metric(
            job_name=job_name,
            status="success",
            rows_read=result.get("rows_read", 0),
            rows_written=result.get("rows_written", 0),
            bad_rows=result.get("bad_rows", 0),
            duration_seconds=duration,
        )

    except Exception as e:
        duration = time.time() - start

        write_metric(
            job_name=job_name,
            status="failed",
            duration_seconds=duration,
            error_message=str(e),
        )

        traceback.print_exc()
        raise
