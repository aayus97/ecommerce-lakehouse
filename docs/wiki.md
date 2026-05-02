# Ecommerce Lakehouse Technical Wiki

This wiki is a first-principles learning guide for this repository. It explains
what the project is doing, why each design choice exists, how the code implements
it, and what you should be able to explain in an interview or portfolio review.

Use it as a study roadmap, a writing source, and a technical map of the repo.

## 1. The Problem This Lakehouse Solves

An ecommerce business creates operational data every time a customer signs up,
views or buys a product, cancels an order, returns an item, or updates account
information. The raw operational records are useful, but they are not immediately
ready for analytics.

From first principles, analytics needs four things:

| Need | Why it matters |
| --- | --- |
| Durable history | Analysts need to reproduce numbers and investigate past states. |
| Clean semantics | A metric like revenue must mean the same thing every time it is queried. |
| Controlled change | Late updates, duplicate files, and reruns should not corrupt tables. |
| Observable execution | Operators need to know what ran, what failed, how many rows moved, and whether data is fresh. |

This project models that problem locally with Spark and Delta Lake. It starts
with raw ecommerce CSV files, stores them as Delta tables, validates and
quarantines bad orders, transforms valid records into trusted silver tables, and
builds gold aggregates for reporting.

The repo is intentionally production-like in the areas that matter for a data
engineering portfolio:

- Layered data architecture: raw, bronze, quarantine, silver, gold.
- Idempotent ingestion: repeated batches should not duplicate orders.
- Incremental Delta merges: new and changed orders are applied by key.
- Data contracts: schemas, grains, primary keys, and metric definitions are documented.
- Data quality controls: invalid rows are isolated with reasons.
- SCD Type 2 dimensions: customer and product changes preserve history.
- Backfills: selected date windows can be rerun.
- Observability: JSONL metrics feed Streamlit, Prometheus, and Grafana.
- Reproducibility: Docker and Make commands package Spark, Delta, tests, and local services.

## 2. Repository Map

The repo is organized by responsibility. Understanding these directories first
helps you reason about the whole system.

| Path | Purpose |
| --- | --- |
| `jobs/` | Executable Spark job modules. Each pipeline step runs one module with `python -m jobs...`. |
| `src/` | Shared Python logic for Spark sessions, config, Delta merge helpers, validation, metrics, privacy, and backfill filtering. |
| `configs/` | Environment and pipeline configuration. `pipeline.yaml` defines execution order and retries. |
| `tests/` | Unit, integration, contract, regression, config, and Delta design tests. |
| `docs/` | Architecture, contracts, data quality, runbooks, security, testing, production readiness, and this wiki. |
| `dashboard/` | Streamlit monitoring app for run status, quality, freshness, and business metrics. |
| `observability/` | Prometheus, Alertmanager, and Grafana configuration. |
| `orchestration/` | Dagster asset definitions that wrap the same pipeline concepts with lineage and scheduling. |
| `seed_data/raw/` | Small committed raw CSV fixtures used to seed local runs. |
| `data/` | Runtime lakehouse data. Ignored by Git. |
| `metrics/` | Runtime metrics and validation summaries. Mostly ignored by Git. |
| `scripts/` | Synthetic data generation and support scripts. |
| `utils/` | Prometheus metric exporter and helper code. |
| `Dockerfile`, `docker-compose.yml`, `Makefile` | Reproducible local runtime, services, and workflow shortcuts. |

Important entry points:

| File | What to study |
| --- | --- |
| `run_pipeline.py` | Config validation, step selection, dependency ordering, retries, run IDs, step metrics, backfill env vars. |
| `src/delta_utils.py` | Order upserts, idempotency, record hashes, SCD2 dimension merge, partition-aware overwrites. |
| `src/order_validation.py` | Data quality rules, quarantine reasons, duplicate checks, referential checks, threshold summary. |
| `src/backfill.py` | Date-window filtering and affected partition calculation. |
| `src/metrics.py` | JSONL metric writing. |
| `src/config.py` | Environment config loading and table path resolution. |

## 3. Lakehouse Fundamentals

### What A Lakehouse Is

A data lake is good at storing many kinds of raw data cheaply, usually as files
in object storage. A data warehouse is good at serving clean, governed,
query-friendly tables. A lakehouse combines those ideas by storing data in open
file formats while adding table-level reliability features.

At the storage level, a lakehouse is still mostly files:

- CSV, JSON, Parquet, or other data files.
- Directories that represent tables and partitions.
- Metadata that tells query engines which files belong to which table version.

At the table level, the lakehouse adds behavior that ordinary files do not have:

- ACID transactions.
- Schema enforcement and evolution controls.
- Upserts and deletes.
- Time travel and history.
- Consistent reads while writes are happening.

### Why Delta Lake Matters

This repo uses Delta Lake because plain Parquet files are not enough for
incremental, reliable pipelines.

If you only write Parquet files, then an upsert is awkward. You need to find old
files, rewrite affected records, avoid duplicate writes, and protect readers
from partial outputs. Delta Lake adds a transaction log beside the data files.
That log records table versions and operations such as `WRITE`, `MERGE`, and
partition replacement.

In this project, Delta matters because:

- Bronze orders are merged by `order_id`.
- SCD2 customer and product dimensions are merged by business key.
- Gold tables can overwrite only affected `order_date` partitions.
- Delta history can prove that expected write and merge operations happened.
- Reruns can be designed around table semantics instead of loose file appends.

### Bronze, Silver, And Gold From First Principles

The lakehouse layers separate concerns.

| Layer | First-principles responsibility | Repo implementation |
| --- | --- | --- |
| Raw | Keep source-shaped input files. | `data/raw/*.csv` seeded from `seed_data/raw/*.csv`. |
| Bronze | Preserve source records in Delta with minimal interpretation and technical metadata. | `data/bronze/orders`, `customers`, `products`. |
| Quarantine | Preserve invalid records outside trusted analytical flow. | `data/quarantine/orders`. |
| Silver | Create typed, cleaned, trusted analytical facts and dimensions. | `data/silver/orders`, `customers`, `products`. |
| Gold | Create business-specific aggregates with clear metric definitions. | `data/gold/daily_sales_summary`, `revenue_by_category_country`. |

The most important principle: do not destroy raw information too early. If the
pipeline filters, casts, or aggregates too soon, it becomes difficult to debug
bad metrics later. Bronze is the audit-friendly memory of what arrived. Silver
is where data becomes reliable enough to join and analyze. Gold is where data
becomes specific to business questions.

## 4. Raw Data And Data Contracts

A data contract is an agreement between a producer and a consumer. It says what
columns exist, what each row represents, what keys identify records, which
columns are required, and what values are valid.

Without contracts, pipelines can silently break:

- A producer renames `customer_id` to `cust_id`.
- A numeric column starts arriving as text.
- A date is sent in an unexpected format.
- A status value changes from `completed` to `complete`.
- A primary key stops being unique.

The repo documents contracts in `docs/data_contracts.md` and tests them in
`tests/test_contract_table_schemas.py`.

### Orders Contract

Raw orders live at `data/raw/orders.csv` or `s3a://lakehouse/raw/orders.csv`.

| Concept | Value |
| --- | --- |
| Grain | One source order record per row. |
| Primary key | `order_id`. |
| Main consumer | `jobs/02_ingest_orders_bronze.py`. |
| Foreign keys | `customer_id`, `product_id`. |
| Business date | `order_date`. |
| Optional freshness field | `update_timestamp`. |

Required columns:

| Column | Micro-level meaning |
| --- | --- |
| `order_id` | Business identity of the order. Used as the Delta merge key. |
| `customer_id` | Links the order to the customer dimension. |
| `product_id` | Links the order to the product dimension. |
| `order_date` | Business date of the order. Used for partitioning and reporting. |
| `quantity` | Number of units ordered. Must be greater than zero. |
| `unit_price` | Selling price per unit. Must be greater than or equal to zero. |
| `status` | Lifecycle state. Must be `completed`, `cancelled`, or `returned`. |

Optional column:

| Column | Why it matters |
| --- | --- |
| `update_timestamp` | Tells the pipeline which version of an order is newest when multiple versions arrive. |

### Customers Contract

Raw customers live at `data/raw/customers.csv`.

| Concept | Value |
| --- | --- |
| Grain | One customer per row. |
| Primary key | `customer_id`. |
| PII columns | `customer_name`, `email`. |

Columns:

| Column | Micro-level meaning |
| --- | --- |
| `customer_id` | Stable customer identity used for joins and SCD2 tracking. |
| `customer_name` | Personally identifiable information. Mask before display. |
| `email` | Personally identifiable information. Lowercased and trimmed in silver. |
| `country` | Analytical geography dimension. |
| `signup_date` | Customer lifecycle date. Cast to date in silver. |

### Products Contract

Raw products live at `data/raw/products.csv`.

| Concept | Value |
| --- | --- |
| Grain | One product per row. |
| Primary key | `product_id`. |

Columns:

| Column | Micro-level meaning |
| --- | --- |
| `product_id` | Stable product identity used for joins and SCD2 tracking. |
| `product_name` | Product label, trimmed in silver. |
| `category` | Analytical product grouping. |
| `unit_cost` | Cost basis, cast to double in silver. |

## 5. Bronze Layer

Bronze is where source data becomes a managed Delta table while still remaining
close to the source.

The orders bronze job is `jobs/02_ingest_orders_bronze.py`. It:

1. Reads raw orders from the configured raw path.
2. Applies optional backfill date filtering.
3. Calls `merge_orders_by_id` in `src/delta_utils.py`.
4. Writes step metrics with input path, output path, row counts, merge key, and write mode.

### Bronze Orders Metadata Columns

`src/delta_utils.py` adds these metadata columns to orders:

| Column | Meaning |
| --- | --- |
| `source_update_ts` | Timestamp representing when the source record changed. Uses `update_timestamp`, `updated_at`, `source_update_ts`, or ingestion time fallback. |
| `ingestion_ts` | Timestamp when this pipeline ingested the row. |
| `ingestion_date` | Date derived from ingestion time. Useful for operational partitioning or audits. |
| `record_hash` | SHA-256 hash of business columns used to detect whether the row content actually changed. |

The business columns used for `record_hash` are:

- `order_id`
- `customer_id`
- `product_id`
- `order_date`
- `quantity`
- `unit_price`
- `status`

The core idea: timestamps alone are not enough. If a batch is rerun and the
timestamp is equal or newer but the business fields are identical, the pipeline
should avoid creating unnecessary changes. `record_hash` gives a compact content
fingerprint.

### Bronze Partitioning

Orders bronze is partitioned by `order_date`.

Partitioning means the table directory is physically organized by a column value,
usually with folders such as:

```text
data/bronze/orders/order_date=2026-01-01/
data/bronze/orders/order_date=2026-01-02/
```

Partitioning helps when queries filter by that column. For ecommerce analytics,
most operational reruns and reports are date-window based, so `order_date` is a
natural partition key.

Bad partition choices create too many tiny folders or do not match query
patterns. Good partition choices match common filters and keep each partition
large enough to be efficient.

## 6. Incremental Processing And Idempotency

Incremental processing means the pipeline processes new or changed records
instead of rebuilding everything from scratch every time.

Idempotency means running the same operation more than once produces the same
logical result. In data pipelines, this is essential because retries and reruns
are normal.

Example:

1. A batch with `order_id = 101` arrives.
2. The pipeline writes it to bronze.
3. The same batch is rerun after a transient failure.
4. Bronze should still contain one current row for `order_id = 101`, not two.

### How Orders Upserts Work

`merge_orders_by_id` implements the bronze order merge.

The logic is:

1. Normalize incoming orders.
2. Cast `order_date` to date.
3. Add metadata columns.
4. Compute `record_hash`.
5. Deduplicate the incoming batch by `order_id`.
6. Keep the newest row per `order_id` using:
   - highest `source_update_ts`;
   - then highest `ingestion_ts`;
   - then highest `record_hash`.
7. If the target Delta table does not exist, create it.
8. If the target table exists, merge on `target.order_id = source.order_id`.
9. Update a matched row only when:
   - `source.source_update_ts >= target.source_update_ts`;
   - and `source.record_hash <> target.record_hash`.
10. Insert rows that do not already exist.

The merge condition protects against three common failure modes:

| Failure mode | Protection |
| --- | --- |
| Duplicate input rows in the same batch | `latest_orders_by_id` keeps one row per `order_id`. |
| Rerunning identical data | Same `record_hash` means no update is needed. |
| Older late-arriving update | Older `source_update_ts` cannot overwrite newer target state. |

### Late Updates

A late update is a record that arrives after newer data has already been
processed. Late updates are common when upstream systems export files late,
retry failed sends, or replay historical events.

This repo treats `source_update_ts` as the ordering signal. If a late-arriving
record has an older source timestamp than the target, it should not replace the
newer state. If it has a newer timestamp, it can update the order even if the
business `order_date` is old.

### Merge Keys

A merge key is the column or set of columns used to match source rows to target
rows.

In this project:

| Table type | Merge key |
| --- | --- |
| Orders | `order_id` |
| Customers dimension | `customer_id` |
| Products dimension | `product_id` |

The merge key must match the table grain. If the table grain is one row per
order, `order_id` is valid. If the grain were one row per order line, the key
would need something like `(order_id, line_id)`.

## 7. Data Quality

Data quality is the system's defense against turning bad input into trusted
metrics.

Order validation is implemented in `src/order_validation.py` and executed by
`jobs/12_validate_and_quarantine_orders.py`.

### Validation Rule Types

| Rule type | Question it answers | Example |
| --- | --- | --- |
| Schema | Does the expected structure exist? | Is `order_id` present? |
| Null | Are required values populated? | Is `customer_id` null? |
| Range | Are numeric values reasonable? | Is `quantity > 0`? |
| Allowed values | Is a categorical value valid? | Is `status` one of the allowed statuses? |
| Date logic | Does the date parse and make business sense? | Is `order_date` in the future? |
| Uniqueness | Does the key identify one record? | Is `order_id` duplicated in the validation batch? |
| Referential | Do foreign keys exist in dimensions? | Does `product_id` exist in bronze products? |
| Threshold | Is the batch quality acceptable overall? | Is invalid percentage <= configured threshold? |

### Order Validation Rules

| Failure reason | Meaning |
| --- | --- |
| `missing_required_column` | A required column is absent from the DataFrame schema. |
| `duplicate_order_id` | More than one row has the same `order_id` in the validation batch. |
| `missing_order_id` | `order_id` is null. |
| `missing_customer_id` | `customer_id` is null. |
| `missing_product_id` | `product_id` is null. |
| `invalid_quantity` | `quantity` is null or less than/equal to zero. |
| `invalid_unit_price` | `unit_price` is null or less than zero. |
| `invalid_status` | `status` is null or not in the allowed set. |
| `future_order_date` | `order_date` does not parse as a date or is greater than the current date. |
| `unknown_customer_id` | `customer_id` does not exist in the customer reference set. |
| `unknown_product_id` | `product_id` does not exist in the product reference set. |
| `invalid_percentage_threshold_exceeded` | Overall invalid row percentage exceeds the configured threshold. |

Allowed statuses:

- `completed`
- `cancelled`
- `returned`

### Invalid Percentage Threshold

The invalid percentage threshold is configured in environment config, for
example `configs/dev.yaml`.

Conceptually:

```text
invalid_percentage = invalid_rows / total_rows * 100
```

If the invalid percentage is below or equal to the threshold, the pipeline can
continue with valid rows while quarantining invalid rows. If it exceeds the
threshold, the validation step fails the pipeline because the batch may indicate
a systemic upstream issue.

This is a practical production pattern. One bad row should not stop an entire
business. A batch where 40% of rows are bad probably should.

### Why Keep All Reasons

The validation code stores:

| Column | Purpose |
| --- | --- |
| `quarantine_reason` | First failure reason, useful for simple grouping and dashboards. |
| `quarantine_reasons` | All failure reasons, useful for debugging and root cause analysis. |

A row can fail multiple rules. For example, an order can have `quantity = -1`,
`status = "done"`, and an unknown `product_id`. Keeping all reasons prevents the
first failure from hiding the rest.

## 8. Quarantine Pattern

Quarantine means invalid rows are isolated instead of deleted.

From first principles, bad data is still evidence. It tells you what the source
sent, when it arrived, and why the pipeline rejected it. Deleting it makes
debugging harder and can hide upstream problems.

The repo writes invalid orders to:

```text
data/quarantine/orders
```

The valid rows continue into:

```text
data/bronze/orders_validated
```

That split gives the system two properties:

| Property | Why it matters |
| --- | --- |
| Safety | Bad rows do not contaminate silver and gold tables. |
| Continuity | Good rows can still power analytics when quality is within threshold. |

### How To Investigate Quarantine

Use the inspection job:

```bash
python -m jobs.13_view_quarantine_orders
```

What to inspect:

- `quarantine_reason`
- `quarantine_reasons`
- original source fields
- row counts by reason
- whether failures cluster by source file, date, product, country, or status

How to think about remediation:

| Finding | Likely action |
| --- | --- |
| Null key fields | Fix upstream export logic or reject source file earlier. |
| Unknown customer/product IDs | Check reference data load timing or source referential integrity. |
| Invalid status | Align source system enum with pipeline contract. |
| Future dates | Check timezone, date parsing, or bad test data. |
| High duplicate rate | Investigate source extraction or merge grain. |

## 9. Silver Layer

Silver is the trusted analytical layer. It is where the project starts applying
business semantics.

### Silver Orders

Implemented in `jobs/03_transform_orders_silver.py`.

Inputs:

```text
data/bronze/orders_validated
```

Outputs:

```text
data/silver/orders
```

Transformations:

| Transformation | Why |
| --- | --- |
| Cast `order_date` to date | Ensures partitioning and reporting use a real date type. |
| Cast `quantity` to int | Makes numeric aggregation predictable. |
| Cast `unit_price` to double | Makes revenue calculation predictable. |
| Compute `total_amount = round(quantity * unit_price, 2)` | Creates a reusable order-level revenue measure. |
| Filter `status == "completed"` | Gold revenue should count completed sales only. |

The silver order grain is one valid completed order per row.

Important distinction:

- Bronze orders can contain completed, cancelled, and returned orders.
- Silver orders keep completed orders for revenue analytics.

That filtering is a business rule. If the business later wants return rate or
cancellation analytics, those metrics may need another silver fact table or a
less filtered order lifecycle table.

### Silver Customers And Products

Implemented in `jobs/06_transform_customers_products_silver.py`.

Customer transformations:

- Trim `customer_name`.
- Lowercase and trim `email`.
- Trim `country`.
- Cast `signup_date` to date.
- Merge as SCD Type 2.
- Mask customer values before printing to console output.

Product transformations:

- Trim `product_name`.
- Trim `category`.
- Cast `unit_cost` to double.
- Merge as SCD Type 2.

## 10. Slowly Changing Dimensions

A dimension describes a business entity such as a customer or product. A slowly
changing dimension is a dimension whose attributes change over time.

Examples:

- A customer changes country.
- A product changes category.
- A product cost changes.
- A customer email is corrected.

If you overwrite the old dimension row, you lose historical context. If you keep
every version without marking the current one, joins become ambiguous. SCD Type 2
solves this by storing multiple versions with validity windows.

### SCD Type 2 Columns

This repo adds these columns to silver dimensions:

| Column | Meaning |
| --- | --- |
| `record_hash` | Hash of business attributes used to detect changes. |
| `source_update_ts` | Source timestamp for the version. |
| `ingestion_ts` | Pipeline ingestion timestamp. |
| `ingestion_date` | Pipeline ingestion date. |
| `valid_from` | Timestamp when this dimension version became valid. |
| `valid_to` | Timestamp when this version stopped being valid. Null means still current. |
| `is_current` | Boolean flag for active version. |

### How The Repo's SCD2 Merge Works

`merge_dimension_scd2` in `src/delta_utils.py` implements the pattern.

The logic:

1. Normalize the incoming dimension records.
2. Compute `record_hash` from business columns.
3. Keep the newest input row per business key.
4. If the target does not exist, write the normalized records as current rows.
5. If the target exists, compare each incoming row to the current target row.
6. If the hash changed, close the current target row:
   - set `valid_to = source.valid_from`;
   - set `is_current = false`.
7. Insert the incoming changed row as the new current version.
8. Insert brand-new keys as current rows.

Micro-level detail: the function creates staged updates and staged inserts. That
lets one Delta merge both close the old current row and insert the new version.

### Why Dimension History Matters

Suppose a customer lived in France in January and Germany in March. If you ask,
"How much revenue came from France in January?" the answer depends on whether
you join orders to the customer country as of the order date or to the current
customer country.

This project's gold revenue job joins to current dimension rows for the current
analytical view. The SCD2 table still preserves the historical versions so the
project can evolve toward as-of joins later.

Interview explanation:

> I used SCD Type 2 for customer and product dimensions so attribute changes are
> auditable and reruns are idempotent. Current gold metrics use `is_current`, but
> the dimension table retains `valid_from` and `valid_to`, which would support
> historical as-of reporting if needed.

## 11. Gold Layer

Gold tables answer specific business questions. They should have explicit grain
and metric definitions.

### Daily Sales Summary

Implemented in `jobs/04_create_gold_sales_summary.py`.

| Concept | Value |
| --- | --- |
| Input | `data/silver/orders` |
| Output | `data/gold/daily_sales_summary` |
| Grain | One row per `order_date` |
| Partitioning | `order_date` |

Metrics:

| Column | Definition |
| --- | --- |
| `total_orders` | Count of completed silver orders. |
| `unique_customers` | Distinct customers with completed orders on that date. |
| `daily_revenue` | Sum of `total_amount`. |

### Revenue By Category And Country

Implemented in `jobs/07_create_gold_revenue_by_category_country.py`.

| Concept | Value |
| --- | --- |
| Inputs | `orders_silver`, `customers_silver`, `products_silver` |
| Output | `data/gold/revenue_by_category_country` |
| Grain | One row per `order_date`, `country`, `category` |
| Partitioning | `order_date` |

Metrics:

| Column | Definition |
| --- | --- |
| `total_orders` | Count of completed orders in the date, country, category group. |
| `unique_customers` | Distinct customers in the same group. |
| `revenue` | Rounded sum of `total_amount`. |

### Grain Is Everything

Grain means "what one row represents."

If you do not define grain, metrics become ambiguous.

Examples:

| Table | Grain |
| --- | --- |
| `orders_silver` | One valid completed order. |
| `daily_sales_summary` | One day. |
| `revenue_by_category_country` | One day, country, and category combination. |

The same metric name can mean different things at different grains. `total_orders`
in daily sales means orders per day. `total_orders` in revenue by category and
country means orders per day-country-category group.

## 12. Backfills

A backfill reruns part of the pipeline for historical data.

Reasons for backfills:

- A bug in silver transformation logic was fixed.
- A data quality rule changed.
- A source sent corrected historical records.
- A gold metric definition changed.
- A failed run left a date window incomplete.

The pipeline supports date-window backfills:

```bash
python run_pipeline.py \
  --start-date 2026-01-01 \
  --end-date 2026-01-03 \
  --steps validate_orders,silver_orders,gold_daily_sales,gold_revenue,collect_gold_metrics
```

### How Backfill Dates Flow

`run_pipeline.py` parses `--start-date` and `--end-date`, validates that the
start is not after the end, and writes them to environment variables:

- `BACKFILL_START_DATE`
- `BACKFILL_END_DATE`

Jobs use helpers in `src/backfill.py` to filter by `order_date` and to report
backfill context in metrics.

### Partition-Aware Overwrites

Gold and some silver backfill writes use `write_date_partitions_delta` from
`src/delta_utils.py`.

The principle:

- Do not overwrite a whole table when only three dates changed.
- Replace only affected `order_date` partitions.
- Keep unrelated partitions available and untouched.

Delta implements this with `replaceWhere`, for example:

```text
order_date IN ('2026-01-01', '2026-01-02', '2026-01-03')
```

This is important because full table overwrites become expensive and risky as
data grows.

## 13. Pipeline Orchestration

The local runner is `run_pipeline.py`. The configured steps live in
`configs/pipeline.yaml`.

### Pipeline Steps

| Step | Module | Purpose |
| --- | --- | --- |
| `ingest_orders_bronze` | `jobs.02_ingest_orders_bronze` | Ingest initial raw orders into bronze with Delta merge. |
| `ingest_customers_products_bronze` | `jobs.05_ingest_customers_products_bronze` | Ingest customer and product raw data into bronze. |
| `bronze_merge` | `jobs.08_incremental_orders_bronze_merge` | Merge incremental order batch into bronze. |
| `check_delta_history` | `jobs.09_check_delta_history` | Verify expected Delta write and merge operations exist. |
| `validate_orders` | `jobs.12_validate_and_quarantine_orders` | Validate bronze orders and split valid/quarantine outputs. |
| `silver_orders` | `jobs.03_transform_orders_silver` | Create typed completed order fact table. |
| `silver_customers_products` | `jobs.06_transform_customers_products_silver` | Create SCD2 customer and product dimensions. |
| `gold_daily_sales` | `jobs.04_create_gold_sales_summary` | Create daily sales aggregate. |
| `gold_revenue` | `jobs.07_create_gold_revenue_by_category_country` | Create revenue aggregate by date, country, and category. |
| `collect_gold_metrics` | `jobs.14_collect_gold_metrics` | Emit business and freshness metrics. |

### Dependency Ordering

The pipeline config defines `depends_on` lists. The runner executes steps in the
configured order and checks that dependencies have completed.

Examples:

- `validate_orders` depends on bronze order merge, Delta history check, and customer/product bronze ingestion.
- `gold_revenue` depends on silver orders plus silver customer/product dimensions.
- `collect_gold_metrics` depends on both gold tables.

This prevents the pipeline from building downstream outputs from missing or
stale upstream tables.

### Retries

Each step has a `retries` count. `run_pipeline.py` attempts a step up to:

```text
1 + retries
```

For each attempt, it writes a `pipeline_steps` metric with:

- `run_id`
- `step`
- `module`
- `status`
- `attempt`
- `max_attempts`
- `duration_seconds`
- `return_code`
- `failure_reason`

Retries are useful for transient failures such as temporary storage or Spark
startup issues. They do not fix deterministic bugs. If validation fails because
40% of rows are invalid, retrying the same bad input will usually fail again.

### Selected Steps

`--steps` lets you run a subset:

```bash
python run_pipeline.py --steps gold_daily_sales,gold_revenue,collect_gold_metrics
```

Dependencies inside the selected set are enforced. Dependencies outside the
selected set are treated as pre-existing. This supports operational reruns from
known-good upstream tables.

### Dagster Assets

The repo also includes Dagster assets in `orchestration/`.

Dagster adds a production-style control plane:

- asset lineage;
- schedules;
- retries;
- run history;
- UI inspection;
- clearer dependency graph.

The important portfolio point: `run_pipeline.py` is a simple local orchestrator,
while Dagster demonstrates how the same lakehouse could be managed by a proper
orchestration framework.

## 14. Observability

Observability means the system explains itself while it runs and after it fails.

The repo emits JSONL metric files under `metrics/`. JSONL means one JSON object
per line. This format is simple to append, easy to inspect with shell tools, and
easy for dashboards/exporters to parse.

### Metric Types

| Metric file/type | Purpose |
| --- | --- |
| `pipeline_runs` | One summary record per pipeline run. |
| `pipeline_steps` | One record per step attempt. |
| `step_metrics` | Row movement and IO metrics per Spark job. |
| `orders_data_quality` | Validation totals, invalid percentage, reason counts, threshold result. |
| `freshness_metrics` | Latest business dates and table update timestamps. |
| `gold_sales_metrics` | Business-level totals such as orders, revenue, and average order value. |

### Run IDs

Every pipeline execution receives a unique `run_id`.

The run ID lets you connect:

- pipeline summary;
- step attempts;
- quality results;
- row movement;
- backfill date windows;
- business metrics.

Without a run ID, you can still see that things happened, but it is harder to
reconstruct one specific execution.

### Step Metrics

Each job writes metrics such as:

- `rows_read`
- `rows_written`
- `rows_quarantined`
- `duration_seconds`
- `input_path`
- `output_path`
- write mode details
- partition details
- backfill details

These answer operational questions:

- Did this job process the expected number of rows?
- Did it write to the expected table?
- Did it quarantine anything?
- Did a backfill touch the intended dates?
- Did runtime suddenly increase?

### Streamlit Dashboard

`dashboard/monitoring_app.py` reads the JSONL metrics and presents:

- selected run status and duration;
- failed step and failure reason;
- retry attempts;
- row counts;
- data quality summary;
- quarantine reasons;
- freshness metrics;
- business metrics;
- historical run trends.

### Prometheus And Grafana

`utils/prometheus_metrics.py` exports local JSONL metrics in Prometheus format.
Prometheus scrapes the exporter. Grafana reads from Prometheus and displays the
observability dashboard configured under `observability/grafana/`.

Alert rules live in:

```text
observability/prometheus/alerts.yml
```

Alertmanager config lives in:

```text
observability/alertmanager/alertmanager.yml
```

Important alert concepts:

| Alert category | First-principles meaning |
| --- | --- |
| Pipeline failure | The latest run did not complete. |
| High invalid percentage | Data quality is outside acceptable tolerance. |
| Stale gold freshness | Business outputs may not reflect recent source data. |
| Missing metrics | The observability system may be blind. |
| Exporter down | Prometheus cannot read the local metric source. |

## 15. Testing Strategy

Tests prove that important assumptions are encoded, not just documented.

The repo includes several kinds of tests:

| Test type | Purpose |
| --- | --- |
| Unit tests | Validate small pure functions such as order validation logic. |
| Integration tests | Run a mini pipeline path to verify components work together. |
| Contract tests | Check expected schemas and table shapes. |
| Regression tests | Protect known business metric outputs from accidental changes. |
| Config validation tests | Ensure pipeline config is structurally valid. |
| Delta design tests | Check idempotent merge and table design behavior. |
| Synthetic data tests | Validate realistic data generation. |

Important files:

| File | What it protects |
| --- | --- |
| `tests/test_unit_core_functions.py` | Core helper behavior. |
| `tests/test_orders_validation_rules.py` | Data quality rule behavior. |
| `tests/test_contract_table_schemas.py` | Contract expectations. |
| `tests/test_integration_mini_pipeline.py` | End-to-end mini pipeline behavior. |
| `tests/test_regression_gold_metrics.py` | Gold metric correctness over known fixtures. |
| `tests/test_pipeline_config.py` | Pipeline config validation and dependency logic. |
| `tests/test_delta_table_design.py` | Delta merge and design guarantees. |

Testing principle: test the things that would create expensive confusion if they
broke silently.

For this project, that means:

- validation rules;
- merge idempotency;
- contract schemas;
- pipeline dependency config;
- gold metric definitions;
- SCD2 behavior;
- backfill partition behavior.

## 16. Security And Governance

Data engineering systems often touch sensitive customer data. Even in a local
portfolio project, security habits matter.

### Environment Files And Secrets

`.env` is ignored by Git. `.env.example` documents expected variables without
storing secrets.

Principle:

- Commit configuration templates.
- Do not commit machine-local credentials.
- Do not commit tokens, passwords, private keys, or real customer data.

### Runtime Data

Generated data under `data/`, runtime logs, and most metric outputs are ignored
by Git. That keeps code review focused on source changes and prevents accidental
commit of local outputs.

### PII Classification

`docs/data_classification.md` classifies customer, order, and product fields.
Customer fields such as `customer_name` and `email` are PII-like and should be
masked before display in logs, screenshots, dashboards, or support output.

Masking helpers live in:

```text
src/privacy.py
```

The silver dimension job uses `mask_customer_columns` before printing customer
records.

### Secret Scanning

Security checks are available through:

```bash
make security
```

This runs local secret scanning tools configured for the project.

## 17. Docker And Reproducibility

Spark and Delta Lake depend on specific Java, Python, PySpark, and Delta package
versions. A pipeline that works on one laptop but fails on another is not
reproducible.

Docker solves that by packaging the runtime:

- Python 3.12
- Java 21
- PySpark 4.0
- Delta Lake 4.0 dependencies
- Dagster
- Streamlit
- pytest
- ruff

Make commands provide repeatable workflows:

| Command | Purpose |
| --- | --- |
| `make install` | Build the local Docker image. |
| `make test` | Run pytest in the container. |
| `make lint` | Run ruff in the container. |
| `make security` | Run local secret checks. |
| `make pipeline` | Seed raw data and run the full pipeline. |
| `make pipeline-realistic` | Generate larger synthetic data and run the pipeline. |
| `make dashboard` | Start Streamlit. |
| `make dagster` | Start Dagster. |
| `make observability` | Start exporter, Prometheus, Alertmanager, and Grafana. |
| `make pipeline-minio` | Run against MinIO object storage. |

Reproducibility is not just convenience. It is part of engineering reliability.
If reviewers, teammates, or interviewers can run the same commands and get the
same behavior, the project is easier to trust.

## 18. Production Readiness

This project is production-like, but it is not a full production deployment.
That distinction is important and useful to explain honestly.

### Production-Like Today

| Area | What exists |
| --- | --- |
| Storage format | Delta tables with merges, history, and partitioned writes. |
| Layering | Raw, bronze, quarantine, silver, gold. |
| Idempotency | Order merges by key and hash; SCD2 dimension merges. |
| Quality | Validation rules, referential checks, thresholds, quarantine. |
| Backfills | Date-window reruns and partition-aware overwrites. |
| Orchestration | Local runner plus Dagster assets. |
| Observability | JSONL metrics, Streamlit, Prometheus, Grafana, alerts. |
| Testing | Unit, integration, contract, regression, config, and design tests. |
| Governance | `.env` hygiene, ignored runtime data, PII classification, masking helpers. |
| Reproducibility | Docker, Compose, and Make workflows. |

### Missing For Real Production

| Area | What would be needed |
| --- | --- |
| Real sources | Connect to actual source systems, CDC streams, APIs, or object storage drops. |
| Managed secrets | Use a real secret manager such as AWS Secrets Manager, Vault, or cloud-native equivalents. |
| Deployment | Run jobs on managed Spark, Kubernetes, or another production compute platform. |
| Access control | Enforce table, path, dashboard, and secret permissions. |
| Retention | Define retention, vacuum, legal hold, and data lifecycle policies. |
| CI/CD | Run tests, linting, security checks, image builds, and deployment promotion automatically. |
| Alert routing | Send alerts to Slack, PagerDuty, email, or incident tooling. |
| Data catalog | Register tables, owners, lineage, and classifications in a catalog. |
| SLA/SLOs | Define freshness, availability, runtime, and quality objectives. |
| Cost controls | Track Spark resource usage, storage growth, file sizes, and compaction needs. |

Good portfolio phrasing:

> This is a local production-style lakehouse. It demonstrates the reliability
> patterns I would use in production, but real deployment would still need
> managed secrets, real source integrations, access control, CI/CD, cataloging,
> retention policies, and routed alerts.

## 19. Portfolio Presentation

When presenting this project, start with the business problem before tools.

### Short Interview Narrative

Use this structure:

1. Problem: ecommerce order, customer, and product data needs reliable analytics.
2. Architecture: raw CSV inputs flow through bronze, quarantine, silver, and gold Delta tables.
3. Reliability: orders use idempotent Delta merges by `order_id`, source timestamp, and record hash.
4. Quality: bad rows are quarantined with reasons, while valid rows continue if thresholds pass.
5. History: customer and product dimensions use SCD Type 2 to preserve changes.
6. Recovery: date-window backfills rebuild affected partitions instead of overwriting everything.
7. Observability: run, step, quality, freshness, and business metrics feed dashboards and alerts.
8. Tradeoffs: current gold joins use current dimensions; production would need managed infrastructure and governance.

### What To Emphasize

| Topic | Strong explanation |
| --- | --- |
| Lakehouse | "I use Delta Lake to bring transactional table behavior to file-based storage." |
| Bronze | "Bronze preserves source-shaped records plus ingestion metadata for audit and reruns." |
| Idempotency | "Rerunning the same batch should not duplicate rows because merges happen by key and content hash." |
| Quarantine | "Invalid rows are isolated with reasons instead of deleted, so debugging and remediation remain possible." |
| Silver | "Silver applies type casting, cleaning, and business filtering to create trusted tables." |
| SCD2 | "Dimensions keep historical versions with `valid_from`, `valid_to`, and `is_current`." |
| Gold | "Gold tables are aggregates with explicit grain and metric definitions." |
| Backfills | "Backfills pass date windows through the pipeline and replace only affected partitions." |
| Observability | "Every run has a run ID and emits operational, quality, freshness, and business metrics." |
| Production readiness | "The patterns are production-like, but real production needs managed secrets, deployment, access control, retention, CI/CD, and alert routing." |

### Tradeoffs To Mention

Honest tradeoffs make the project more credible:

- Current inputs are local CSVs, not real CDC or event streams.
- Gold revenue joins use current dimension records, while historical as-of joins would be a future improvement.
- Local JSONL metrics are simple and transparent, while production would likely use centralized logging and monitoring.
- The local Docker runtime is reproducible, while production would need managed Spark or orchestrated compute.
- Partitioning by `order_date` fits this project, but larger datasets would require file compaction and partition-size monitoring.

## 20. Suggested Writing Order

If you are writing a README section, blog post, portfolio page, or interview
script, use this order:

1. What problem does this lakehouse solve?
2. What data enters the system, and what contracts protect it?
3. How does raw data become bronze Delta tables?
4. Why are Bronze, Silver, Gold separated?
5. How does validation work, and why quarantine bad rows?
6. How do incremental Delta merges make order ingestion idempotent?
7. How do SCD Type 2 dimensions preserve customer and product history?
8. What do the gold tables calculate, and what is their grain?
9. How do backfills rerun date windows safely?
10. How does orchestration manage dependencies, retries, and selected steps?
11. What metrics make the pipeline observable?
12. What tests prove the behavior?
13. What security and governance practices are included?
14. What is production-like today, and what would be improved next?

## 21. Micro-Level Concept Checklist

Use this checklist to test whether you truly understand the project.

### Lakehouse

- Can you explain why a Delta table is more than a folder of Parquet files?
- Can you explain why raw data should be preserved?
- Can you explain the difference between raw and bronze?
- Can you explain why silver should be trusted but not overly aggregated?
- Can you explain why gold tables need explicit grain?

### Contracts

- Can you define the grain of orders, customers, products, and gold tables?
- Can you identify primary keys and foreign keys?
- Can you explain what happens when a required column is missing?
- Can you explain how contract tests reduce silent breakage?

### Bronze

- Can you explain `source_update_ts`, `ingestion_ts`, `ingestion_date`, and `record_hash`?
- Can you explain why `order_date` is used as the partition column?
- Can you explain how batch deduplication works before Delta merge?
- Can you explain why a content hash helps idempotency?

### Incremental Processing

- Can you explain what an upsert is?
- Can you explain the difference between append, overwrite, and merge?
- Can you explain why rerunning the same batch should be harmless?
- Can you explain how late updates are accepted or rejected?

### Data Quality

- Can you list each validation rule and what failure it catches?
- Can you explain null, range, allowed-value, duplicate, and referential checks?
- Can you explain invalid percentage thresholds?
- Can you explain why one row can have multiple quarantine reasons?

### Quarantine

- Can you explain why invalid rows are isolated instead of deleted?
- Can you explain how quarantine supports debugging?
- Can you explain how valid rows continue safely?

### Silver

- Can you explain why type casting happens in silver?
- Can you explain why completed orders are filtered for revenue analytics?
- Can you explain how customer email and country are cleaned?
- Can you explain why PII should be masked before display?

### SCD2

- Can you explain `is_current`, `valid_from`, and `valid_to`?
- Can you explain how `record_hash` detects dimension changes?
- Can you explain what happens when a customer attribute changes?
- Can you explain the difference between current joins and historical as-of joins?

### Gold

- Can you define `daily_sales_summary` grain and metrics?
- Can you define `revenue_by_category_country` grain and metrics?
- Can you explain why aggregations should be built from silver, not raw?
- Can you explain why metric definitions belong in documentation and tests?

### Backfills

- Can you explain a date-window rerun?
- Can you explain affected partitions?
- Can you explain `replaceWhere`?
- Can you explain why partition-aware overwrites are safer than full overwrites?

### Orchestration

- Can you explain how `configs/pipeline.yaml` controls the pipeline?
- Can you explain dependency ordering?
- Can you explain retries and their limits?
- Can you explain selected step reruns?
- Can you explain why Dagster assets are useful beyond a simple script?

### Observability

- Can you explain why every run needs a `run_id`?
- Can you explain pipeline, step, quality, freshness, and business metrics?
- Can you explain why JSONL is convenient locally?
- Can you explain how Streamlit, Prometheus, Grafana, and Alertmanager fit together?

### Testing

- Can you explain the difference between unit, integration, contract, and regression tests?
- Can you explain what behavior would be risky to leave untested?
- Can you explain why config validation should be tested?

### Security

- Can you explain why `.env` is ignored?
- Can you explain which columns are PII?
- Can you explain why runtime data should not be committed?
- Can you explain why secret scanning belongs in the workflow?

## 22. Study Prompts

Use these prompts to deepen your understanding:

1. What would break if bronze used append-only writes instead of merge?
2. What would break if `record_hash` did not exist?
3. What would break if validation deleted invalid rows instead of quarantining them?
4. What would change if order grain became one row per order line?
5. What would change if products could belong to multiple categories?
6. How would you implement historical as-of joins for SCD2 dimensions?
7. How would you handle a source that sends deletes?
8. How would you design compaction and vacuum policies for large Delta tables?
9. How would you promote this from local Docker to cloud object storage and managed Spark?
10. What SLAs would you define for freshness, quality, and runtime?

## 23. Quick Command Reference

Run the main workflow:

```bash
make install
make test
make lint
make security
make pipeline
```

Validate pipeline config:

```bash
python run_pipeline.py --validate-only
```

Run a backfill:

```bash
python run_pipeline.py \
  --start-date 2026-01-01 \
  --end-date 2026-01-03 \
  --steps validate_orders,silver_orders,gold_daily_sales,gold_revenue,collect_gold_metrics
```

Inspect quarantine:

```bash
python -m jobs.13_view_quarantine_orders
```

Start dashboard:

```bash
make dashboard
```

Start Dagster:

```bash
make dagster
```

Start Prometheus and Grafana observability:

```bash
make observability
```

Run against MinIO:

```bash
make pipeline-minio
```

## 24. Related Docs

- `docs/architecture.md`: system architecture and data flow.
- `docs/data_contracts.md`: schemas, grains, keys, and metric contracts.
- `docs/data_quality.md`: validation rules, quarantine behavior, and quality metrics.
- `docs/data_classification.md`: PII and governance notes.
- `docs/testing_strategy.md`: test layers and what they protect.
- `docs/operations_runbook.md`: running, inspecting, recovering, and alerting.
- `docs/production_readiness.md`: production-style capabilities and screenshots.
- `docs/production_readiness_plan.md`: improvement roadmap.
