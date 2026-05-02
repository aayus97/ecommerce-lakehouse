# Operations Runbook

This runbook covers the local and prod-like workflows for running, recovering,
and inspecting the ecommerce lakehouse pipeline.

## Prerequisites

The most reproducible path uses Docker and Docker Compose:

```bash
make install
```

For local Python execution:

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

Optional local overrides live in `.env`. Never commit `.env`.

## Run The Pipeline

Run the full local pipeline:

```bash
make pipeline
```

Equivalent local Python command after preparing `data/raw`:

```bash
python run_pipeline.py
```

Validate the pipeline configuration without executing jobs:

```bash
python run_pipeline.py --validate-only
```

Run a date-window backfill across trusted layers:

```bash
python run_pipeline.py \
  --start-date 2026-01-01 \
  --end-date 2026-01-03 \
  --steps validate_orders,silver_orders,gold_daily_sales,gold_revenue,collect_gold_metrics
```

Run only selected gold rebuilds from existing silver tables:

```bash
python run_pipeline.py \
  --start-date 2026-01-01 \
  --end-date 2026-01-03 \
  --steps gold_daily_sales,gold_revenue,collect_gold_metrics
```

The selected steps run in the order defined by `configs/pipeline.yaml`.
Dependencies inside the selected set are enforced. Dependencies outside the
selected set are treated as pre-existing inputs, which lets operational backfills
reuse already-built bronze, silver, or dimension tables.

Run against MinIO object storage:

```bash
make pipeline-minio
```

The runner writes a unique `run_id` into each run and records run, step, quality,
freshness, and business metrics in `metrics/*.jsonl`.

## Inspect A Run

Use the dashboard for the fastest operational view:

```bash
make dashboard
```

Open `http://localhost:8501`. The dashboard shows run status, failed step,
retry attempts, failure reason, row movement, quarantine counts, freshness, and
business metrics.

Inspect raw metric files:

```bash
tail -n 20 metrics/pipeline_runs.jsonl
tail -n 20 metrics/pipeline_steps.jsonl
tail -n 20 metrics/orders_data_quality.jsonl
tail -n 20 metrics/step_metrics.jsonl
```

## Recover A Failed Pipeline

1. Identify the failed run.

```bash
tail -n 5 metrics/pipeline_runs.jsonl
```

Look for `status`, `failed_step`, `failure_reason`, and `run_id`.

2. Inspect the failed step attempts.

```bash
grep '<run_id>' metrics/pipeline_steps.jsonl
```

Use the failed `step`, `module`, `return_code`, and `failure_reason` to decide
whether the issue is input data, configuration, Spark/Delta runtime, or output
storage.

3. Fix the root cause.

Common fixes:

| Symptom | Likely cause | Recovery |
| --- | --- | --- |
| Config validation fails. | Missing step field, duplicate name, bad dependency, or invalid retry value. | Fix `configs/pipeline.yaml`, then run `python run_pipeline.py --validate-only`. |
| Raw path not found. | `data/raw` not seeded or MinIO seed missing. | Run `make seed-data` locally or `make minio-seed` for MinIO. |
| Validation threshold failure. | Too many invalid orders. | Inspect quarantine, fix upstream records, adjust only with explicit business approval. |
| Delta history check fails. | Bronze table did not receive expected Delta write/merge operations. | Confirm `orders_bronze` exists and rerun ingestion/merge from clean inputs if needed. |
| Gold table failure. | Missing silver inputs, schema drift, or join issue. | Verify silver tables and data contracts, then rerun after fixing upstream step. |

4. Rerun the pipeline.

```bash
make pipeline
```

Order ingestion is idempotent by `order_id` and `record_hash`, so rerunning the
same input batch should not duplicate orders. Silver customer and product
dimensions merge changes as SCD Type 2 records, and gold jobs rebuild from the
current trusted silver inputs.

5. Confirm recovery.

Check that the latest `pipeline_runs` record has `status: success`, that failed
step counts are zero, and that the dashboard shows expected row movement and
freshness.

## Inspect Quarantine

Run the quarantine inspection job:

```bash
python -m jobs.13_view_quarantine_orders
```

Or inspect the Delta files with Spark:

```python
from src.config import load_app_config, table_path
from src.spark_session import get_spark

config = load_app_config()
spark = get_spark("InspectQuarantine")
spark.read.format("delta").load(table_path(config, "orders_quarantine")).show(
    truncate=False
)
spark.stop()
```

For local file-level checks, the quarantine table is under:

```text
data/quarantine/orders
```

Focus on `quarantine_reason`, `quarantine_reasons`, and the original order
columns. After fixing upstream input data, rerun the pipeline and confirm the
invalid count decreases.

## Start Observability Services

Run the metrics exporter, Prometheus, and Grafana:

```bash
make observability
```

Open:

- Metrics exporter: `http://localhost:9108/metrics`
- Prometheus: `http://localhost:9090`
- Alertmanager: `http://localhost:9093`
- Grafana: `http://localhost:3000`

Grafana is provisioned with the `Ecommerce Lakehouse Observability` dashboard.
Prometheus loads alert rules from `observability/prometheus/alerts.yml` and
sends firing alerts to Alertmanager using
`observability/alertmanager/alertmanager.yml`.

## Alert Reference

| Alert | Meaning | First checks |
| --- | --- | --- |
| `LakehousePipelineRunFailed` | The latest pipeline run did not complete successfully. | Inspect `metrics/pipeline_runs.jsonl`, then inspect the failed step in `metrics/pipeline_steps.jsonl`. |
| `LakehouseHighInvalidOrderPercentage` | The latest data quality run exceeded the invalid order threshold. | Inspect quarantine rows and `metrics/orders_data_quality.jsonl`. |
| `LakehouseGoldFreshnessStale` | Gold table freshness is more than 2 hours old. | Confirm the latest pipeline run completed and `collect_gold_metrics` ran. |
| `LakehousePipelineMetricsMissing` | No pipeline run metrics are available to scrape. | Run the pipeline and confirm `metrics/pipeline_runs.jsonl` exists. |
| `LakehouseDataQualityMetricsMissing` | No order quality metrics are available to scrape. | Confirm `validate_orders` ran and wrote `metrics/orders_data_quality.jsonl`. |
| `LakehouseGoldMetricsMissing` | No gold business metrics are available to scrape. | Confirm `collect_gold_metrics` ran and wrote `metrics/gold_sales_metrics.jsonl`. |
| `LakehouseMetricsExporterDown` | Prometheus cannot scrape the exporter. | Confirm the `metrics-exporter` container is running and `http://localhost:9108/metrics` responds. |

For local notification testing, open Alertmanager at `http://localhost:9093`.
The default receiver stores alerts in the Alertmanager UI only. To send alerts
externally, replace or extend the `local-observability` receiver with a Slack,
email, PagerDuty, webhook, or cloud monitoring receiver.

## Stop Services

```bash
make down
```
