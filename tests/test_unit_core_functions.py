import json

import pytest

from run_pipeline import validate_config
from src.config import path_value, table_path
from src.metrics import write_metric
from src.order_validation import write_validation_summary


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
