import argparse
import json
import sys
from datetime import date, datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.config import load_app_config, path_value  # noqa: E402

EXPECTED_METRIC_NAMES = [
    "pipeline_runs",
    "pipeline_steps",
    "step_metrics",
    "orders_data_quality",
    "gold_sales_metrics",
    "freshness_metrics",
]


def load_jsonl_metrics(metrics_dir):
    records = []
    for path in sorted(Path(metrics_dir).glob("*.jsonl")):
        with path.open("r") as file:
            for line in file:
                if line.strip():
                    records.append(json.loads(line))
    return records


def label_value(value):
    return str(value or "").replace("\\", "\\\\").replace('"', '\\"')


def labels(**values):
    clean_values = {key: value for key, value in values.items() if value is not None}
    if not clean_values:
        return ""
    rendered = ",".join(
        f'{key}="{label_value(value)}"' for key, value in sorted(clean_values.items())
    )
    return f"{{{rendered}}}"


def number(value, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def timestamp_seconds(value):
    if not value:
        return None

    if isinstance(value, date) and not isinstance(value, datetime):
        parsed = datetime(value.year, value.month, value.day, tzinfo=timezone.utc)
    else:
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)

    return parsed.timestamp()


def record_sort_timestamp(record):
    for field in ("timestamp", "ended_at", "started_at"):
        timestamp = timestamp_seconds(record.get(field))
        if timestamp is not None:
            return timestamp
    return 0


def latest_record(records, metric_name):
    candidates = [
        record for record in records if record.get("metric_name") == metric_name
    ]
    if not candidates:
        return None
    return max(candidates, key=record_sort_timestamp)


def generate_prometheus_text(records):
    lines = [
        "# HELP lakehouse_pipeline_run_status Pipeline run status by run ID.",
        "# TYPE lakehouse_pipeline_run_status gauge",
        "# HELP lakehouse_latest_pipeline_run_failed Latest pipeline run failed flag.",
        "# TYPE lakehouse_latest_pipeline_run_failed gauge",
        "# HELP lakehouse_pipeline_run_duration_seconds Pipeline run duration.",
        "# TYPE lakehouse_pipeline_run_duration_seconds gauge",
        "# HELP lakehouse_pipeline_step_duration_seconds Pipeline step duration.",
        "# TYPE lakehouse_pipeline_step_duration_seconds gauge",
        "# HELP lakehouse_step_rows Row movement by pipeline step.",
        "# TYPE lakehouse_step_rows gauge",
        "# HELP lakehouse_orders_invalid_count Invalid order count.",
        "# TYPE lakehouse_orders_invalid_count gauge",
        "# HELP lakehouse_orders_invalid_percentage Invalid order percentage.",
        "# TYPE lakehouse_orders_invalid_percentage gauge",
        "# HELP lakehouse_latest_orders_invalid_percentage Latest invalid order percentage.",
        "# TYPE lakehouse_latest_orders_invalid_percentage gauge",
        "# HELP lakehouse_business_metric Gold-layer business metric.",
        "# TYPE lakehouse_business_metric gauge",
        "# HELP lakehouse_freshness_timestamp_seconds Data freshness timestamp.",
        "# TYPE lakehouse_freshness_timestamp_seconds gauge",
        "# HELP lakehouse_metrics_records_total Number of JSONL metric records by type.",
        "# TYPE lakehouse_metrics_records_total gauge",
    ]

    metric_counts = {metric_name: 0 for metric_name in EXPECTED_METRIC_NAMES}
    for record in records:
        metric_name = record.get("metric_name")
        if metric_name in metric_counts:
            metric_counts[metric_name] += 1

    for metric_name, count_value in metric_counts.items():
        lines.append(
            "lakehouse_metrics_records_total"
            f"{labels(metric_name=metric_name)} {count_value}"
        )

    latest_pipeline_run = latest_record(records, "pipeline_runs")
    if latest_pipeline_run:
        status = str(latest_pipeline_run.get("status") or "unknown")
        lines.append(
            "lakehouse_latest_pipeline_run_failed"
            f"{labels(pipeline=latest_pipeline_run.get('pipeline_name'), run_id=latest_pipeline_run.get('run_id'), status=status, failed_step=latest_pipeline_run.get('failed_step'))} "
            f"{0 if status == 'success' else 1}"
        )

    latest_quality = latest_record(records, "orders_data_quality")
    if latest_quality:
        lines.append(
            "lakehouse_latest_orders_invalid_percentage"
            f"{labels(pipeline=latest_quality.get('pipeline_name'), run_id=latest_quality.get('run_id'))} "
            f"{number(latest_quality.get('invalid_percentage'))}"
        )

    for record in records:
        metric_name = record.get("metric_name")
        common = {
            "pipeline": record.get("pipeline_name"),
            "run_id": record.get("run_id"),
        }

        if metric_name == "pipeline_runs":
            status = str(record.get("status") or "unknown")
            lines.append(
                "lakehouse_pipeline_run_status"
                f"{labels(**common, status=status)} {1 if status == 'success' else 0}"
            )
            lines.append(
                "lakehouse_pipeline_run_duration_seconds"
                f"{labels(**common, status=status)} {number(record.get('duration_seconds'))}"
            )

        if metric_name == "pipeline_steps":
            lines.append(
                "lakehouse_pipeline_step_duration_seconds"
                f"{labels(**common, step=record.get('step'), status=record.get('status'), attempt=record.get('attempt'))} "
                f"{number(record.get('duration_seconds'))}"
            )

        if metric_name == "step_metrics":
            for field, row_type in [
                ("rows_read", "read"),
                ("rows_written", "written"),
                ("rows_quarantined", "quarantined"),
            ]:
                lines.append(
                    "lakehouse_step_rows"
                    f"{labels(**common, step=record.get('step'), row_type=row_type)} "
                    f"{number(record.get(field))}"
                )

        if metric_name == "orders_data_quality":
            lines.append(
                "lakehouse_orders_invalid_count"
                f"{labels(**common)} {number(record.get('invalid_count', record.get('invalid_rows')))}"
            )
            lines.append(
                "lakehouse_orders_invalid_percentage"
                f"{labels(**common)} {number(record.get('invalid_percentage'))}"
            )

        if metric_name == "gold_sales_metrics":
            total_revenue = number(record.get("total_revenue"))
            total_orders = number(record.get("total_orders"))
            average_order_value = record.get("average_order_value")
            if average_order_value is None and total_orders:
                average_order_value = round(total_revenue / total_orders, 2)

            business_values = {
                "total_revenue": total_revenue,
                "total_orders": total_orders,
                "average_order_value": average_order_value,
            }

            for field, value in business_values.items():
                lines.append(
                    "lakehouse_business_metric"
                    f"{labels(**common, metric=field)} {number(value)}"
                )

        if metric_name in {"gold_sales_metrics", "freshness_metrics"}:
            for field in ["latest_order_date", "gold_table_last_updated_timestamp"]:
                timestamp = timestamp_seconds(record.get(field))
                if timestamp is not None:
                    lines.append(
                        "lakehouse_freshness_timestamp_seconds"
                        f"{labels(**common, metric=field, source=metric_name)} "
                        f"{timestamp}"
                    )

    return "\n".join(lines) + "\n"


class MetricsHandler(BaseHTTPRequestHandler):
    metrics_dir = None

    def do_GET(self):
        if self.path != "/metrics":
            self.send_response(404)
            self.end_headers()
            return

        body = generate_prometheus_text(load_jsonl_metrics(self.metrics_dir)).encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; version=0.0.4")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def main():
    config = load_app_config()
    parser = argparse.ArgumentParser(
        description="Serve lakehouse metrics for Prometheus"
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9108)
    parser.add_argument("--metrics-dir", default=path_value(config, "metrics"))
    args = parser.parse_args()

    MetricsHandler.metrics_dir = args.metrics_dir
    server = ThreadingHTTPServer((args.host, args.port), MetricsHandler)
    print(f"Serving Prometheus metrics at http://{args.host}:{args.port}/metrics")
    server.serve_forever()


if __name__ == "__main__":
    main()
