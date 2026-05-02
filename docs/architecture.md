# Architecture

This project is a local ecommerce lakehouse built with Spark, Delta Lake, Docker,
Dagster, Streamlit, Prometheus, and Grafana. The pipeline follows the standard
bronze, silver, gold pattern and adds quarantine, metrics, and dashboarding so
pipeline health is visible alongside business outputs.

## System Architecture

```mermaid
flowchart LR
    raw["Raw CSV seed data<br/>data/raw or s3a://lakehouse/raw"]
    spark["Spark + Delta jobs<br/>jobs/*.py"]
    bronze["Bronze Delta<br/>orders, customers, products"]
    quarantine["Quarantine Delta<br/>orders with validation failures"]
    silver["Silver Delta<br/>typed and trusted facts/dimensions"]
    gold["Gold Delta<br/>business aggregates"]
    metrics["JSONL metrics<br/>metrics/*.jsonl"]
    streamlit["Streamlit dashboard<br/>dashboard/monitoring_app.py"]
    prometheus["Prometheus exporter<br/>utils/prometheus_metrics.py"]
    grafana["Grafana dashboard<br/>observability/grafana"]
    dagster["Dagster assets<br/>orchestration/*"]

    raw --> spark
    spark --> bronze
    bronze --> quarantine
    bronze --> silver
    silver --> gold
    spark --> metrics
    gold --> metrics
    metrics --> streamlit
    metrics --> prometheus
    prometheus --> grafana
    dagster --> spark
```

## Data Flow

```mermaid
flowchart TD
    orders_raw["orders.csv"] --> ingest_orders["ingest_orders_bronze"]
    orders_batch["orders_batch_2.csv"] --> bronze_merge["bronze_merge"]
    customers_raw["customers.csv"] --> ingest_dims["ingest_customers_products_bronze"]
    products_raw["products.csv"] --> ingest_dims

    ingest_orders --> orders_bronze["bronze.orders<br/>Delta, partitioned by order_date"]
    bronze_merge --> orders_bronze
    orders_bronze --> delta_history["check_delta_history"]
    orders_bronze --> validate["validate_orders"]
    validate --> orders_validated["bronze.orders_validated<br/>valid rows"]
    validate --> orders_quarantine["quarantine.orders<br/>invalid rows + reasons"]

    ingest_dims --> customers_bronze["bronze.customers"]
    ingest_dims --> products_bronze["bronze.products"]
    customers_bronze --> silver_dims["silver_customers_products"]
    products_bronze --> silver_dims
    silver_dims --> customers_silver["silver.customers"]
    silver_dims --> products_silver["silver.products"]

    orders_validated --> silver_orders_job["silver_orders"]
    silver_orders_job --> orders_silver["silver.orders<br/>completed orders + total_amount"]

    orders_silver --> daily_sales_job["gold_daily_sales"]
    orders_silver --> revenue_job["gold_revenue"]
    customers_silver --> revenue_job
    products_silver --> revenue_job
    daily_sales_job --> daily_sales["gold.daily_sales_summary"]
    revenue_job --> revenue["gold.revenue_by_category_country"]

    daily_sales --> collect_metrics["collect_gold_metrics"]
    revenue --> collect_metrics
    orders_silver --> collect_metrics
```

## Runtime Modes

| Mode | Config | Storage | How to run |
| --- | --- | --- | --- |
| Local dev | `configs/dev.yaml` | `data/*` local Delta paths | `make pipeline` or `python run_pipeline.py` |
| Test | `configs/test.yaml` | temporary local paths from tests | `make test` |
| MinIO | `configs/minio.yaml` | `s3a://lakehouse/*` object storage paths | `make pipeline-minio` |

`run_pipeline.py` reads `configs/pipeline.yaml`, validates step definitions and
dependency order, assigns a `run_id`, executes enabled jobs, retries failed
steps according to config, and writes run and step metrics to JSONL.

Dagster assets in `orchestration/` provide a production-style orchestration
surface for lineage, schedules, retries, and run history while preserving the
same underlying Spark job modules.

## Storage Layers

| Layer | Purpose | Primary paths |
| --- | --- | --- |
| Raw | Source CSV inputs copied from `seed_data/raw` for local runs. | `data/raw/*.csv` |
| Bronze | Delta-preserved source data with order upserts and technical metadata. | `data/bronze/orders`, `data/bronze/customers`, `data/bronze/products` |
| Quarantine | Invalid orders isolated before silver and gold processing. | `data/quarantine/orders` |
| Silver | Typed, cleaned, reporting-ready facts and dimensions. | `data/silver/orders`, `data/silver/customers`, `data/silver/products` |
| Gold | Business aggregates used by dashboards and regression tests. | `data/gold/daily_sales_summary`, `data/gold/revenue_by_category_country` |
| Metrics | Operational, quality, freshness, and business metric events. | `metrics/*.jsonl` |

Generated lakehouse data and metrics are runtime artifacts and should not be
committed.
