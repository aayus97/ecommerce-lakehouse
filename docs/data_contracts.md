# Data Contracts

The table contracts below document the expected schema, grain, ownership, and
quality expectations for each major dataset. Tests in
`tests/test_contract_table_schemas.py` enforce the raw fixture and gold table
schemas.

## Raw Inputs

### `orders_raw`

| Property | Value |
| --- | --- |
| Path | `data/raw/orders.csv` or `s3a://lakehouse/raw/orders.csv` |
| Grain | One source order record per row |
| Primary key | `order_id` |
| Consumer | `jobs/02_ingest_orders_bronze.py` |

| Column | Type | Required | Notes |
| --- | --- | --- | --- |
| `order_id` | int/string-compatible | yes | Business key used for Delta upserts. |
| `customer_id` | int/string-compatible | yes | Customer foreign key. |
| `product_id` | int/string-compatible | yes | Product foreign key. |
| `order_date` | date/string-compatible | yes | Business order date. |
| `quantity` | numeric | yes | Must be greater than 0. |
| `unit_price` | numeric | yes | Must be greater than or equal to 0. |
| `status` | string | yes | Must be `completed`, `cancelled`, or `returned`. |
| `update_timestamp` | timestamp/string-compatible | optional | Used as source freshness when present. |

### `customers_raw`

| Property | Value |
| --- | --- |
| Path | `data/raw/customers.csv` or `s3a://lakehouse/raw/customers.csv` |
| Grain | One customer per row |
| Primary key | `customer_id` |
| Consumer | `jobs/05_ingest_customers_products_bronze.py` |

| Column | Type | Required | Notes |
| --- | --- | --- | --- |
| `customer_id` | int/string-compatible | yes | Customer key. |
| `customer_name` | string | yes | PII. Mask in logs, examples, and screenshots. |
| `email` | string | yes | PII. Lowercased and trimmed in silver. |
| `country` | string | yes | Trimmed in silver. |
| `signup_date` | date/string-compatible | yes | Cast to date in silver. |

### `products_raw`

| Property | Value |
| --- | --- |
| Path | `data/raw/products.csv` or `s3a://lakehouse/raw/products.csv` |
| Grain | One product per row |
| Primary key | `product_id` |
| Consumer | `jobs/05_ingest_customers_products_bronze.py` |

| Column | Type | Required | Notes |
| --- | --- | --- | --- |
| `product_id` | int/string-compatible | yes | Product key. |
| `product_name` | string | yes | Trimmed in silver. |
| `category` | string | yes | Trimmed in silver. |
| `unit_cost` | numeric | yes | Cast to double in silver. |

## Bronze Tables

### `orders_bronze`

| Property | Value |
| --- | --- |
| Path | `data/bronze/orders` or `s3a://lakehouse/bronze/orders` |
| Format | Delta |
| Grain | Latest known order state by `order_id` |
| Partitioning | `order_date` |
| Writer | `merge_orders_by_id` in `src/delta_utils.py` |

| Column | Type | Notes |
| --- | --- | --- |
| `order_id` | source-compatible | Merge key. |
| `customer_id` | source-compatible | Customer foreign key. |
| `product_id` | source-compatible | Product foreign key. |
| `order_date` | date | Partition column. |
| `quantity` | source-compatible numeric | Preserved until validation/silver typing. |
| `unit_price` | source-compatible numeric | Preserved until validation/silver typing. |
| `status` | string | Source order status. |
| `source_update_ts` | timestamp | Source update timestamp or ingestion fallback. |
| `ingestion_ts` | timestamp | Time the row was ingested by the pipeline. |
| `ingestion_date` | date | Ingestion date. |
| `record_hash` | string | Hash of business columns for idempotent merge detection. |

Upserts deduplicate each input batch by `order_id`, keeping the newest
`source_update_ts`, then update existing target rows only when the source is as
fresh or fresher and the business hash changed.

### `customers_bronze` and `products_bronze`

| Table | Path | Grain | Notes |
| --- | --- | --- | --- |
| `customers_bronze` | `data/bronze/customers` | One customer per row | Raw customer CSV persisted as Delta. |
| `products_bronze` | `data/bronze/products` | One product per row | Raw product CSV persisted as Delta. |

## Quarantine

### `orders_quarantine`

| Property | Value |
| --- | --- |
| Path | `data/quarantine/orders` or `s3a://lakehouse/quarantine/orders` |
| Format | Delta |
| Grain | One invalid order row per validation failure event |
| Writer | `jobs/12_validate_and_quarantine_orders.py` |

The quarantine table includes the original order columns plus:

| Column | Type | Notes |
| --- | --- | --- |
| `quarantine_reason` | string | First validation reason for quick filtering. |
| `quarantine_reasons` | array/string list | All validation reasons found for the row. |

## Silver Tables

### `orders_silver`

| Property | Value |
| --- | --- |
| Path | `data/silver/orders` or `s3a://lakehouse/silver/orders` |
| Format | Delta |
| Grain | One valid completed order per row |
| Partitioning | `order_date` |
| Writer | `jobs/03_transform_orders_silver.py` |

| Column | Type | Notes |
| --- | --- | --- |
| `order_id` | source-compatible | Order key. |
| `customer_id` | source-compatible | Customer foreign key. |
| `product_id` | source-compatible | Product foreign key. |
| `order_date` | date | Partition and reporting date. |
| `quantity` | int | Cast in silver. |
| `unit_price` | double | Cast in silver. |
| `status` | string | Filtered to `completed`. |
| `total_amount` | double | `round(quantity * unit_price, 2)`. |

### `customers_silver`

| Column | Type | Notes |
| --- | --- | --- |
| `customer_id` | source-compatible | Customer key. |
| `customer_name` | string | Trimmed. Mask before display outside controlled tables. |
| `email` | string | Lowercased and trimmed. Mask before display. |
| `country` | string | Trimmed. |
| `signup_date` | date | Cast in silver. |

### `products_silver`

| Column | Type | Notes |
| --- | --- | --- |
| `product_id` | source-compatible | Product key. |
| `product_name` | string | Trimmed. |
| `category` | string | Trimmed. |
| `unit_cost` | double | Cast in silver. |

## Gold Tables

### `daily_sales_summary`

| Property | Value |
| --- | --- |
| Path | `data/gold/daily_sales_summary` |
| Format | Delta |
| Grain | One row per `order_date` |
| Writer | `jobs/04_create_gold_sales_summary.py` |
| Write mode | Partition-aware Delta overwrite by `order_date` |

| Column | Type | Definition |
| --- | --- | --- |
| `order_date` | date | Silver order date. |
| `total_orders` | bigint | Count of completed orders. |
| `unique_customers` | bigint | Distinct customers with completed orders. |
| `daily_revenue` | double | Sum of `total_amount`. |

### `revenue_by_category_country`

| Property | Value |
| --- | --- |
| Path | `data/gold/revenue_by_category_country` |
| Format | Delta |
| Grain | One row per `order_date`, `country`, `category` |
| Writer | `jobs/07_create_gold_revenue_by_category_country.py` |
| Write mode | Partition-aware Delta overwrite by `order_date` |

| Column | Type | Definition |
| --- | --- | --- |
| `order_date` | date | Silver order date. |
| `country` | string | Customer country. |
| `category` | string | Product category. |
| `total_orders` | bigint | Count of completed orders. |
| `unique_customers` | bigint | Distinct customers in the date, country, and category group. |
| `revenue` | double | Rounded sum of `total_amount`. |

## Metric Contracts

Metrics are JSONL records in `metrics/*.jsonl`. Every record is expected to have
a `metric_name` and timestamp, with additional fields by metric type.

| Metric | Purpose | Key fields |
| --- | --- | --- |
| `pipeline_runs` | Final run status. | `run_id`, `status`, `duration_seconds`, `failed_step`, `failure_reason`, retry counts. |
| `pipeline_steps` | Per-attempt execution status. | `run_id`, `step`, `module`, `status`, `attempt`, `return_code`, `failure_reason`. |
| `step_metrics` | Per-step row and IO metrics. | `step`, `rows_read`, `rows_written`, `rows_quarantined`, `input_path`, `output_path`. |
| `orders_data_quality` | Validation summary. | `total_rows`, `valid_rows`, `invalid_rows`, `invalid_percentage`, `quarantine_reason_counts`. |
| `freshness_metrics` | Gold freshness signal. | `latest_order_date`, `gold_table_last_updated_timestamp`. |
| `gold_sales_metrics` | Business summary. | `total_orders`, `total_revenue`, `average_order_value`, top countries/categories. |
