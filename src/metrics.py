import json
import os
from datetime import datetime


def write_metric(metric_name: str, data: dict):
    os.makedirs("metrics", exist_ok=True)

    metric_record = {
        "metric_name": metric_name,
        "timestamp": datetime.now().isoformat(),
        **data,
    }

    file_path = f"metrics/{metric_name}.jsonl"

    with open(file_path, "a") as f:
        f.write(json.dumps(metric_record) + "\n")