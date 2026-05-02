# Data Quality

Order validation is implemented in `src/order_validation.py` and executed by
`jobs/12_validate_and_quarantine_orders.py`. The pipeline separates bad rows
before silver and gold processing, writes invalid rows to quarantine, and emits
metrics so quality can be monitored over time.

## Validation Rules

| Rule | Level | Failure reason |
| --- | --- | --- |
| Required order columns are present. | schema | `missing_required_column` |
| `order_id` is unique within the validation batch. | uniqueness | `duplicate_order_id` |
| `order_id` is not null. | null | `missing_order_id` |
| `customer_id` is not null. | null | `missing_customer_id` |
| `product_id` is not null. | null | `missing_product_id` |
| `quantity` is greater than 0. | range | `invalid_quantity` |
| `unit_price` is greater than or equal to 0. | range | `invalid_unit_price` |
| `status` is allowed. | business rule | `invalid_status` |
| `order_date` parses as a date and is not in the future. | business rule | `future_order_date` |
| `customer_id` exists in the supplied customer reference set. | referential | `unknown_customer_id` |
| `product_id` exists in the supplied product reference set. | referential | `unknown_product_id` |
| Invalid row percentage does not exceed the configured threshold. | business rule | `invalid_percentage_threshold_exceeded` |

The allowed order statuses are:

- `completed`
- `cancelled`
- `returned`

## Validation Threshold

The default invalid row threshold is configured in `configs/dev.yaml`:

```yaml
data_quality:
  invalid_percentage_threshold: 5.0
  orders_validation_summary_path: metrics/orders_validation_summary.json
```

Rows below or equal to the threshold are quarantined and the valid subset
continues through the pipeline. If the invalid percentage exceeds the threshold,
the validation job raises an exception after writing metrics and available
outputs, causing the pipeline run to fail.

## Quarantine Behavior

Invalid rows are written to `orders_quarantine` with two diagnostic fields:

| Column | Purpose |
| --- | --- |
| `quarantine_reason` | First failure reason for simple filtering and summary counts. |
| `quarantine_reasons` | All failure reasons for the row. |

The quarantine table is for inspection and remediation. Gold tables are built
from `orders_validated` and `orders_silver`, not from quarantined rows.

## Quality Metrics

The validation job writes:

- `metrics/orders_validation_summary.json`: a readable summary document with
  expectations, counts, threshold result, and rule failure counts.
- `metrics/orders_data_quality.jsonl`: run-scoped quality metrics for the
  Streamlit dashboard, Prometheus exporter, and Grafana dashboard.
- `metrics/step_metrics.jsonl`: row movement metrics, including
  `rows_quarantined`.

Important fields:

| Field | Meaning |
| --- | --- |
| `total_rows` | Rows read from `orders_bronze`. |
| `valid_rows` | Rows that passed validation. |
| `invalid_rows` | Rows written to quarantine. |
| `invalid_percentage` | Invalid rows divided by total rows. |
| `threshold_passed` | Whether the invalid percentage is within threshold. |
| `quarantine_reason_counts` | Count by first quarantine reason. |
| `all_quarantine_reason_counts` | Count by every reason attached to invalid rows. |

## Dashboard Monitoring

The Streamlit dashboard reads `metrics/*.jsonl` and shows:

- selected run status and duration;
- failed step and failure reason;
- per-step row counts and retries;
- total, valid, invalid, and invalid percentage order counts;
- rule-level quarantine failures;
- freshness and business metrics;
- run history and duration trends.

Dashboard screenshots should be refreshed whenever the dashboard layout or
metric contract changes. Store image assets under `docs/assets/` and reference
them from `docs/production_readiness.md`.

## Privacy Notes

Customer data contains PII-like fields in real-world deployments. Use
`src/privacy.py` helpers before displaying `customer_name` or `email` in logs,
dashboards, screenshots, support output, or documentation examples. See
`docs/data_classification.md` for the column-level classification.
