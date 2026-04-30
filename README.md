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

Generated lakehouse data and metrics are ignored by Git so code changes stay separate from runtime output.

## Pipeline Steps

The orchestrated pipeline is defined in `configs/pipeline.yaml`.

1. `bronze_merge`: incrementally merges new orders into bronze.
2. `validate_orders`: validates bronze orders and quarantines invalid rows.
3. `silver_orders`: creates the cleaned silver orders table.
4. `gold_daily_sales`: creates daily sales aggregates.
5. `gold_revenue`: creates revenue aggregates by category and country.
6. `collect_gold_metrics`: writes business metrics for monitoring.

The runner validates the config before execution, including required fields, duplicate step names, retry values, module existence, dependency ordering, and dependency cycles.

## Setup

Create and activate a virtual environment:

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

## Run The Pipeline

```bash
python run_pipeline.py
```

Each run gets a unique `run_id`. Step results and run summaries are written to the configured metrics path, which defaults to `metrics/*.jsonl` in `dev`.

## Run With Dagster

`run_pipeline.py` remains available as a simple local runner. For production-style orchestration, Dagster assets are defined in `orchestration/`.

```bash
dagster dev -w workspace.yaml
```

Open the Dagster UI at the local URL printed by the command, usually `http://127.0.0.1:3000`.

Dagster provides:

- Asset dependencies for bronze merge, validation, silver transform, both gold tables, and metrics collection.
- Per-asset retry policies matching the pipeline config.
- A daily schedule named `daily_ecommerce_lakehouse_schedule`.
- Visible run history, logs, asset lineage, retries, and dependency-aware execution.


## Run Tests

```bash
python -m pytest -q
```

## Launch The Dashboard

```bash
streamlit run dashboard/monitoring_app.py
```

Use the sidebar to filter metric types, time ranges, and pipeline runs. The dashboard highlights failed runs, step failures, data quality trends, and gold/business metrics.

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
