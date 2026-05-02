# Production Readiness Plan

## Phase 0: Define The Target

This document defines the production target for the ecommerce lakehouse before adding more implementation detail. The goal is to make the pipeline measurable: everyone should know what business questions it supports, when the data is expected to arrive, what counts as failure, and how each environment should behave.

## 1. Business Use Case

The lakehouse supports ecommerce reporting and operational data quality decisions across bronze, silver, gold, and quarantine layers.

The primary business decisions are:

- Daily revenue tracking: understand total revenue, order count, and unique customer activity by order date.
- Invalid order detection: identify orders that cannot be trusted because required fields are missing, quantities are invalid, prices are invalid, or statuses are outside the accepted order lifecycle.
- Customer performance: compare customer contribution by revenue, frequency, and geography once customer data is joined to orders.
- Product performance: compare product and category contribution by revenue and order volume once product data is joined to orders.
- Country and category sales: understand which countries and product categories drive revenue and order volume.
- Pipeline health decisions: decide whether gold tables are safe to use for reporting based on pipeline status, data quality metrics, and freshness.

The expected analytical outputs are:

- `data/gold/daily_sales_summary`: daily sales totals, order counts, and unique customers.
- `data/gold/revenue_by_category_country`: revenue, order counts, and unique customers grouped by order date, country, and product category.
- `data/quarantine/orders`: invalid orders separated from trusted reporting tables for inspection and remediation.
- `metrics/*.jsonl`: run, step, data quality, and business metrics used by the monitoring dashboard.

From first principles, the business target is not "run Spark jobs." The target is to turn raw ecommerce events into trusted decision tables. Bronze preserves incoming data, silver makes it valid and typed, gold answers business questions, and quarantine prevents bad rows from silently polluting metrics.

## 2. Service Expectations

### Run Cadence

The production target is a daily batch pipeline. A daily cadence is enough for the current gold tables because they aggregate by order date and support daily revenue, product, customer, country, and category reporting.

Future hourly runs may be added if the business requires same-day operational monitoring, but hourly processing is not the Phase 0 target.

### Late Data Tolerance

The pipeline should accept late-arriving order data up to 24 hours after the business day closes. Late records inside this window should be merged into bronze and reflected in silver and gold on the next successful run.

Records arriving more than 24 hours late are still retained in bronze, but they should be flagged for review before they are allowed to change published gold numbers. This keeps routine lateness automatic while making unusual backfills explicit.

### Failed Run Definition

A run is considered failed when any required enabled step fails after its configured retries, or when a required dependency is not satisfied.

Examples of failed runs:

- Bronze merge cannot read or write the orders table.
- Validation cannot produce `data/bronze/orders_validated`.
- Invalid orders are detected but cannot be written to quarantine.
- Silver transformation cannot read validated bronze data or write silver orders.
- A gold table cannot be rebuilt from silver.
- Pipeline config validation fails before execution.
- A required step is skipped because an upstream dependency did not complete.

A run can complete successfully with quarantined rows as long as invalid orders are isolated, metrics are written, and the valid records continue through silver and gold. Bad input data is a data quality event; failure to isolate and report it is a pipeline failure.

### Gold Freshness

Gold tables are expected to be fresh by the start of the next business reporting window after the daily run.

Initial service target:

- Daily pipeline starts after the prior business day is complete.
- Gold tables are refreshed within 2 hours of pipeline start.
- Monitoring metrics are available within 15 minutes of the pipeline completing.
- Dashboard users should be able to identify the latest run ID, status, failed step if any, invalid order percentage, and gold metric timestamp.

### Data Quality Expectations

Orders are valid only when:

- `order_id`, `customer_id`, and `product_id` are present.
- `quantity` is present and greater than 0.
- `unit_price` is present and not negative.
- `status` is one of `completed`, `cancelled`, or `returned`.

Invalid rows must be written to quarantine and counted in data quality metrics. The gold layer must be built from validated data, not directly from raw bronze orders.

## 3. Environments

### Dev

Dev is for local development and fast feedback.

Expected behavior:

- Runs locally against `data/` paths.
- Uses small generated or checked sample data.
- Allows developers to rerun jobs frequently.
- May overwrite local Delta tables.
- Should write local metrics for debugging.
- Should support focused tests with `pytest`.

Dev success means a developer can run the pipeline, inspect outputs, and verify validation behavior without external infrastructure.

### Test

Test is for deterministic validation.

Expected behavior:

- Uses fixed sample data with known expected outputs.
- Covers valid orders, invalid orders, duplicate or incremental orders, and dimension joins.
- Verifies config validity, validation rules, quarantine behavior, and gold aggregates.
- Produces repeatable results independent of run time or local machine state.
- Avoids relying on previously generated local data.

Test success means the pipeline logic is correct and repeatable for known inputs.

### Prod-Like

Prod-like is for realistic operational rehearsal before production.

Expected behavior:

- Uses the same pipeline structure and step ordering as production.
- Uses larger generated datasets for orders, customers, and products.
- Exercises incremental bronze merge, validation, quarantine, silver transforms, gold aggregates, metrics, and dashboard visibility.
- Tests retry behavior, dependency handling, and failure reporting.
- Measures runtime, output size, invalid row percentage, and gold freshness.

Prod-like success means the pipeline behaves like production at a larger scale, even if the data is generated rather than real.

## Phase 0 Acceptance Criteria

Phase 0 is complete when:

- The business decisions supported by the lakehouse are explicitly documented.
- The run cadence, late data tolerance, failed run definition, and gold freshness target are documented.
- Dev, test, and prod-like environments have clear purposes and expected behaviors.
- Future implementation work can be evaluated against these targets instead of subjective readiness.
