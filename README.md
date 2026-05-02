# Ecommerce Lakehouse

A local Spark and Delta Lake project that models an ecommerce data pipeline with bronze, silver, and gold layers. The project includes incremental order ingestion, data quality validation, quarantine handling, business metrics, pipeline metrics, and a Streamlit monitoring dashboard.

## Architecture

- `data/bronze`: raw and incrementally merged Delta tables.
- `data/bronze/orders_validated`: clean orders after validation.
- `data/quarantine/orders`: invalid orders written for inspection.
- `data/silver`: typed and business-filtered tables.
- `data/gold`: aggregate tables for reporting.
- `metrics/*.jsonl`: pipeline, step, data quality, and business metrics.
- `dashboard/monitoring_app.py`: Streamlit dashboard for pipeline observability.
- `docs/data_classification.md`: customer, order, and product classification notes.

Generated lakehouse data and metrics are ignored by Git so code changes stay separate from runtime output.

## Pipeline Steps

The orchestrated pipeline is defined in `configs/pipeline.yaml`.

1. `ingest_orders_bronze`: upserts the initial bronze orders Delta table by `order_id`.
2. `ingest_customers_products_bronze`: seeds customer and product bronze Delta tables.
3. `bronze_merge`: incrementally merges new orders into bronze with idempotent upserts.
4. `check_delta_history`: checks Delta history for audit-visible write and merge operations.
5. `validate_orders`: validates bronze orders and quarantines invalid rows.
6. `silver_orders`: creates the cleaned silver orders table.
7. `silver_customers_products`: creates cleaned customer and product dimensions.
8. `gold_daily_sales`: creates partition-aware daily sales aggregates.
9. `gold_revenue`: creates partition-aware revenue aggregates by order date, category, and country.
10. `collect_gold_metrics`: writes business metrics for monitoring.

The runner validates the config before execution, including required fields, duplicate step names, retry values, module existence, dependency ordering, and dependency cycles.

## Setup

The fastest reproducible path only needs Docker, Docker Compose, and Make:

```bash
make install
make test
make lint
make security
make pipeline
```

`make pipeline` seeds `data/raw` from `seed_data/raw` before running the full Spark and Delta Lake pipeline in Docker.

The Docker image includes:

- Python 3.12
- Java 21
- PySpark 4.0
- Delta Lake 4.0 dependencies
- Dagster, Streamlit, pytest, and ruff

## Make Targets

```bash
make install      # Build the local Docker image
make test         # Run pytest in the pipeline container
make lint         # Run ruff in the pipeline container
make security     # Run detect-secrets and gitleaks checks
make pipeline     # Seed raw CSVs and run the full pipeline
make pipeline-minio # Run the pipeline against MinIO object storage
make dashboard    # Start Streamlit at http://localhost:8501
make dagster      # Start Dagster at http://localhost:3001
```

Optional supporting services:

```bash
make observability # Metrics exporter, Prometheus, and Grafana
make minio         # MinIO object-store sandbox
make minio-seed    # Create the lakehouse bucket and upload raw seed CSVs
make down          # Stop Compose services
```

## Local Python Setup

Create and activate a virtual environment:

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

Copy `.env.example` to `.env` for local overrides. `.env` is ignored by Git and should contain only machine-local values or credentials. The application loads `.env` via `python-dotenv` before reading environment-based settings.

## Security And Governance

This repo keeps runtime data, logs, metrics, and `.env` files out of Git. Customer fields such as `customer_name` and `email` are classified as PII in `docs/data_classification.md`; order and product fields include column-level PII notes there as well.

Customer name and email masking helpers live in `src/privacy.py`. Jobs that display customer records should use masked values for console output, dashboards, screenshots, or support logs.

Run the local security checks before sharing changes:

```bash
make security
```

`detect-secrets` is installed with Python dependencies. `gitleaks` is expected to be installed on the host because it is a standalone CLI.

## Run The Pipeline

```bash
make pipeline
```

Or, with a local Python environment:

```bash
python run_pipeline.py
```

Run a partition backfill for a date window:

```bash
python run_pipeline.py \
  --start-date 2026-01-01 \
  --end-date 2026-01-03 \
  --steps validate_orders,silver_orders,gold_daily_sales,gold_revenue,collect_gold_metrics
```

`--steps` accepts comma-separated pipeline step names. Dependencies between
selected steps are still honored in pipeline order; dependencies outside the
selected set are treated as already available from previous runs.

Each run gets a unique `run_id`. Run summaries, retry attempts, step IO metrics, data quality metrics, freshness metrics, and business metrics are written to the configured metrics path, which defaults to `metrics/*.jsonl` in `dev`.

Orders Bronze, validated orders, Silver orders, and Gold tables are partitioned by `order_date`.
Order writes deduplicate each batch by `order_id`, keep the newest `source_update_ts`,
and merge only changed records so rerunning the same batch does not duplicate or corrupt
the Delta tables. If upstream data does not provide `update_timestamp`, the pipeline
uses ingestion time plus a business-column hash to make repeated identical batches stable.
Gold jobs replace only the affected `order_date` partitions when rebuilding reporting
tables, which avoids full table overwrites as data volume grows.
Backfill date windows are passed to bronze, validation, silver, and gold order
jobs so selected reruns only process matching `order_date` records.

## Object Storage Mode

Local development uses `configs/dev.yaml` and writes Delta tables under `data/`.
To run the same lakehouse against MinIO, use:

```bash
make pipeline-minio
```

This starts MinIO, creates the `lakehouse` bucket, uploads `seed_data/raw/*.csv` to `s3a://lakehouse/raw/`, and runs Spark with `APP_ENV=minio STORAGE_MODE=minio`.
The MinIO config writes lakehouse tables to:

- `s3a://lakehouse/bronze/...`
- `s3a://lakehouse/silver/...`
- `s3a://lakehouse/gold/...`
- `s3a://lakehouse/quarantine/...`

Open the MinIO console at `http://localhost:9001` with the values from `.env` or the local compose defaults `local-dev-user` / `local-dev-password`.
When running Spark outside Docker, point S3A at the host port:

```bash
APP_ENV=minio STORAGE_MODE=minio MINIO_ENDPOINT=http://localhost:9000 python run_pipeline.py
```

## Run With Dagster

`run_pipeline.py` remains available as a simple local runner. For production-style orchestration, Dagster assets are defined in `orchestration/`.

```bash
make dagster
```

Or, with a local Python environment:

```bash
dagster dev -w workspace.yaml
```

Open the Dagster UI at `http://localhost:3001` when using Make, or at the local URL printed by `dagster dev` when running locally.

If port `3001` is already in use, choose another host port:

```bash
DAGSTER_PORT=3002 make dagster
```

Dagster provides:

- Asset dependencies for bronze merge, validation, silver transform, both gold tables, and metrics collection.
- Per-asset retry policies matching the pipeline config.
- A daily schedule named `daily_ecommerce_lakehouse_schedule`.
- Visible run history, logs, asset lineage, retries, and dependency-aware execution.


## Run Tests

```bash
make test
```

## Launch The Dashboard

```bash
make dashboard
```

If port `8501` is already in use, choose another host port:

```bash
DASHBOARD_PORT=8502 make dashboard
```

Use the sidebar to filter metric types, time ranges, and pipeline runs. The dashboard highlights failed runs, step failures, data quality trends, and gold/business metrics.

The dashboard is designed to answer the production questions quickly:

- What failed: run status, failed step, return code, and failure reason.
- When it failed: run and step start/end timestamps.
- Why it failed: the captured failure reason plus data quality rule counts.
- How bad it is: rows read, written, quarantined, invalid percentage, retries, and business/freshness impact.

## Prometheus And Grafana

JSONL remains the local source of truth. To expose those records to Prometheus:

```bash
python -m utils.prometheus_metrics --host 127.0.0.1 --port 9108
```

Scrape `http://127.0.0.1:9108/metrics` from Prometheus. Grafana can then chart the exported `lakehouse_*` series for pipeline status, step durations, row movement, invalid counts, business totals, and freshness timestamps.

To run the metrics exporter, Prometheus, and Grafana locally:

```bash
make observability
```

Open:

- Metrics exporter: `http://localhost:9108/metrics`
- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3000`

Grafana is provisioned with:

- Data source: Prometheus at `http://prometheus:9090`
- Dashboard: `Ecommerce Lakehouse Observability`
- Login: `admin` / `admin`

To run the Python exporter without Docker:

```bash
python -m utils.prometheus_metrics --host 127.0.0.1 --port 9108
```

If Prometheus shows the target as down, confirm the exporter is running and that `http://127.0.0.1:9108/metrics` returns `lakehouse_*` metrics on your machine.

## Development Notes

- Keep validation rules in `src/order_validation.py` so Spark jobs and tests share the same logic.
- Avoid committing generated Delta tables, Spark metadata, `.crc` files, and metrics output.
- Add tests when changing orchestration behavior, validation rules, or pipeline config structure.

## Environment Configs

The project uses `APP_ENV` to choose environment-specific settings from `configs/`.

```bash
APP_ENV=dev python run_pipeline.py
APP_ENV=test python run_pipeline.py
APP_ENV=prod python run_pipeline.py
```
