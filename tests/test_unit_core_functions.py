import json
import logging
from datetime import date
from pathlib import Path

import pytest
import yaml

from run_pipeline import (
    EXIT_CONFIG_FAILURE,
    EXIT_JOB_FAILURE,
    EXIT_SUCCESS,
    EXIT_VALIDATION_FAILURE,
    classify_failure_exit_code,
    dependencies_satisfied,
    extract_error_type,
    extract_stack_trace,
    parse_date_arg,
    parse_steps_arg,
    selected_steps_or_error,
    validate_config,
)
from src.config import load_app_config, path_value, storage_mode, table_path
from src.logger import StructuredJsonFormatter
from src.metrics import write_metric
from src.order_validation import write_validation_summary
from src.privacy import mask_customer_record, mask_email, mask_name
from utils.prometheus_metrics import generate_prometheus_text


def test_path_generation_returns_configured_table_and_path():
    config = {
        "paths": {"metrics": "/tmp/example/metrics"},
        "tables": {"orders_bronze": "/tmp/example/bronze/orders"},
    }

    assert table_path(config, "orders_bronze") == "/tmp/example/bronze/orders"
    assert path_value(config, "metrics") == "/tmp/example/metrics"


def test_missing_path_generation_key_fails_loudly():
    with pytest.raises(KeyError, match="tables.orders_silver"):
        table_path({"tables": {}}, "orders_silver")

    with pytest.raises(KeyError, match="paths.gold"):
        path_value({"paths": {}}, "gold")


def test_storage_mode_override_selects_minio_config(monkeypatch):
    monkeypatch.setenv("APP_ENV", "dev")
    monkeypatch.setenv("STORAGE_MODE", "minio")

    config = load_app_config()

    assert storage_mode(config) == "minio"
    assert table_path(config, "orders_bronze") == "s3a://lakehouse/bronze/orders"


def test_config_validation_rejects_missing_pipeline_name():
    config = {
        "pipeline": {
            "steps": [
                {
                    "name": "validate",
                    "module": "jobs.12_validate_and_quarantine_orders",
                    "enabled": True,
                }
            ]
        }
    }

    assert "pipeline.name is required" in validate_config(config)


def test_pipeline_exit_codes_are_stable():
    assert EXIT_SUCCESS == 0
    assert EXIT_VALIDATION_FAILURE == 1
    assert EXIT_JOB_FAILURE == 2
    assert EXIT_CONFIG_FAILURE == 3


def test_structured_json_formatter_includes_searchable_context():
    formatter = StructuredJsonFormatter()
    record = logging.LogRecord(
        name="pipeline",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="Completed step",
        args=(),
        exc_info=None,
    )
    record.run_id = "run-123"
    record.step = "silver_orders"
    record.duration_seconds = 12.4

    payload = json.loads(formatter.format(record))

    assert payload["level"] == "INFO"
    assert payload["run_id"] == "run-123"
    assert payload["step"] == "silver_orders"
    assert payload["message"] == "Completed step"
    assert payload["duration_seconds"] == 12.4
    assert "timestamp" in payload


def test_failed_subprocess_trace_parsing_extracts_error_context():
    stderr = "\n".join(
        [
            "Traceback (most recent call last):",
            '  File "job.py", line 1, in <module>',
            "ValueError: bad input",
        ]
    )

    assert extract_error_type(stderr) == "ValueError"
    assert extract_stack_trace(stderr).startswith("Traceback")


def test_validation_step_failures_use_validation_exit_code():
    assert (
        classify_failure_exit_code("validate_orders", "Orders data quality failed")
        == EXIT_VALIDATION_FAILURE
    )
    assert (
        classify_failure_exit_code("silver_orders", "Spark failed") == EXIT_JOB_FAILURE
    )


def test_backfill_cli_parsing_helpers_validate_dates_and_steps():
    assert parse_date_arg("2026-01-02") == date(2026, 1, 2)
    assert parse_steps_arg("silver_orders,gold_daily_sales") == [
        "silver_orders",
        "gold_daily_sales",
    ]

    with pytest.raises(Exception, match="YYYY-MM-DD"):
        parse_date_arg("01-02-2026")


def test_selected_step_validation_rejects_unknown_steps():
    config = {
        "pipeline": {
            "steps": [
                {"name": "silver_orders"},
                {"name": "gold_daily_sales"},
            ]
        }
    }

    selected_steps, errors = selected_steps_or_error(
        config,
        ["silver_orders", "missing_step"],
    )

    assert selected_steps is None
    assert errors == ["Unknown selected step: missing_step"]


def test_selected_run_dependencies_only_require_selected_upstream_steps():
    step = {
        "name": "gold_daily_sales",
        "depends_on": ["silver_orders", "silver_customers_products"],
    }

    assert dependencies_satisfied(step, set(), {"gold_daily_sales"}) == (True, None)
    assert dependencies_satisfied(
        step,
        set(),
        {"silver_orders", "gold_daily_sales"},
    ) == (False, "silver_orders")


def test_metric_writer_appends_jsonl_record(tmp_path, monkeypatch):
    import src.metrics as metrics_module

    monkeypatch.setattr(metrics_module, "METRICS_DIR", tmp_path)
    monkeypatch.setenv("PIPELINE_NAME", "unit-test-pipeline")
    monkeypatch.setenv("PIPELINE_RUN_ID", "run-123")

    write_metric("unit_metric", {"rows": 3})

    records = (tmp_path / "unit_metric.jsonl").read_text().splitlines()
    assert len(records) == 1

    metric = json.loads(records[0])
    assert metric["metric_name"] == "unit_metric"
    assert metric["pipeline_name"] == "unit-test-pipeline"
    assert metric["run_id"] == "run-123"
    assert metric["rows"] == 3
    assert "timestamp" in metric


def test_validation_summary_writer_creates_parent_directory(tmp_path):
    report_path = tmp_path / "nested" / "orders_validation_summary.json"
    summary = {"total_rows": 2, "valid_rows": 2}

    write_validation_summary(str(report_path), summary)

    assert json.loads(report_path.read_text()) == summary


def test_customer_pii_masking_helpers_are_stable():
    assert mask_email("Alice.Johnson@Example.com") == "a***@example.com"
    assert mask_email("not-an-email") == "***"
    assert mask_name("Alice Johnson") == "A*** J***"

    masked = mask_customer_record(
        {
            "customer_id": 101,
            "customer_name": "Alice Johnson",
            "email": "alice@example.com",
            "country": "France",
        }
    )

    assert masked == {
        "customer_id": 101,
        "customer_name": "A*** J***",
        "email": "a***@example.com",
        "country": "France",
    }


def test_prometheus_export_includes_pipeline_quality_and_business_metrics():
    text = generate_prometheus_text(
        [
            {
                "metric_name": "pipeline_runs",
                "pipeline_name": "unit-test",
                "run_id": "run-1",
                "status": "success",
                "duration_seconds": 12.5,
            },
            {
                "metric_name": "orders_data_quality",
                "pipeline_name": "unit-test",
                "run_id": "run-1",
                "invalid_count": 2,
                "invalid_percentage": 10.0,
            },
            {
                "metric_name": "gold_sales_metrics",
                "pipeline_name": "unit-test",
                "run_id": "run-1",
                "total_revenue": 42.0,
                "total_orders": 3,
                "average_order_value": 14.0,
            },
        ]
    )

    assert (
        'lakehouse_pipeline_run_status{pipeline="unit-test",run_id="run-1",'
        'status="success"} 1'
    ) in text
    assert (
        'lakehouse_orders_invalid_count{pipeline="unit-test",run_id="run-1"} 2.0'
    ) in text
    assert (
        'lakehouse_business_metric{metric="total_revenue",pipeline="unit-test",'
        'run_id="run-1"} 42.0'
    ) in text


def test_prometheus_export_includes_alert_friendly_latest_metrics():
    text = generate_prometheus_text(
        [
            {
                "timestamp": "2026-01-01T00:00:00+00:00",
                "metric_name": "pipeline_runs",
                "pipeline_name": "unit-test",
                "run_id": "old-failed-run",
                "status": "failed",
                "failed_step": "silver_orders",
            },
            {
                "timestamp": "2026-01-01T01:00:00+00:00",
                "metric_name": "pipeline_runs",
                "pipeline_name": "unit-test",
                "run_id": "new-success-run",
                "status": "success",
            },
            {
                "timestamp": "2026-01-01T01:01:00+00:00",
                "metric_name": "orders_data_quality",
                "pipeline_name": "unit-test",
                "run_id": "new-success-run",
                "invalid_percentage": 6.5,
            },
        ]
    )

    assert (
        'lakehouse_latest_pipeline_run_failed{pipeline="unit-test",'
        'run_id="new-success-run",status="success"} 0'
    ) in text
    assert (
        'lakehouse_latest_orders_invalid_percentage{pipeline="unit-test",'
        'run_id="new-success-run"} 6.5'
    ) in text
    assert 'lakehouse_metrics_records_total{metric_name="pipeline_runs"} 2' in text
    assert 'lakehouse_metrics_records_total{metric_name="gold_sales_metrics"} 0' in text


def test_prometheus_alert_rules_are_configured():
    alerts_path = Path("observability/prometheus/alerts.yml")
    config = yaml.safe_load(alerts_path.read_text())
    alert_names = {
        rule["alert"]
        for group in config["groups"]
        for rule in group["rules"]
        if "alert" in rule
    }

    assert {
        "LakehousePipelineRunFailed",
        "LakehouseHighInvalidOrderPercentage",
        "LakehouseGoldFreshnessStale",
        "LakehousePipelineMetricsMissing",
        "LakehouseDataQualityMetricsMissing",
        "LakehouseGoldMetricsMissing",
        "LakehouseMetricsExporterDown",
    } <= alert_names


def test_prometheus_is_configured_to_send_alerts_to_alertmanager():
    config = yaml.safe_load(Path("observability/prometheus/prometheus.yml").read_text())

    targets = config["alerting"]["alertmanagers"][0]["static_configs"][0]["targets"]

    assert "alertmanager:9093" in targets


def test_alertmanager_local_receiver_is_configured():
    config = yaml.safe_load(
        Path("observability/alertmanager/alertmanager.yml").read_text()
    )
    receiver_names = {receiver["name"] for receiver in config["receivers"]}

    assert config["route"]["receiver"] == "local-observability"
    assert "local-observability" in receiver_names
