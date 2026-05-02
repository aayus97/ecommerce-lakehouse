# Data Classification

This project is local, but it should still model responsible data engineering habits. Generated Delta tables, raw data copies, metrics, logs, and `.env` files are runtime artifacts and must stay out of Git.

## Classification Levels

| Level | Meaning | Handling |
| --- | --- | --- |
| Public | Safe to publish. | Can be committed when useful. |
| Internal | Operational or business data without direct personal identifiers. | Commit only as small synthetic examples. |
| Confidential | Customer or order data that can identify, contact, or profile a person. | Do not commit real data. Mask in logs, examples, screenshots, and dashboards unless explicitly needed. |
| Secret | Credentials, tokens, private keys, passwords. | Never commit. Store in `.env`, environment variables, or a secret manager. |

## Customer Data

Customer data is **Confidential** when it contains personal identifiers or contact details. The sample files are synthetic, but the same schema in a real setting would need stronger controls.

| Column | Classification | PII | Notes |
| --- | --- | --- | --- |
| `customer_id` | Confidential | Potentially PII | Internal identifier that can link to a person across datasets. |
| `customer_name` | Confidential | PII | Direct personal identifier. Mask in logs and non-production displays. |
| `email` | Confidential | PII | Direct contact identifier. Mask in logs and non-production displays. |
| `country` | Internal | Non-PII | Coarse geography; can become sensitive when combined with other identifiers. |
| `signup_date` | Internal | Non-PII | Lifecycle attribute; avoid exposing with direct identifiers unless needed. |

## Order Data

Order data is **Internal** by default and **Confidential** when joined to customers or when it can reveal individual behavior.

| Column | Classification | PII | Notes |
| --- | --- | --- | --- |
| `order_id` | Internal | Non-PII | Transaction identifier. Treat as confidential if externally linkable. |
| `customer_id` | Confidential | Potentially PII | Foreign key to customer records. |
| `product_id` | Internal | Non-PII | Product foreign key. |
| `order_date` | Internal | Non-PII | Behavioral timestamp; sensitive when tied to a customer. |
| `quantity` | Internal | Non-PII | Purchase measure. |
| `unit_price` | Internal | Non-PII | Commercial measure. |
| `status` | Internal | Non-PII | Operational state such as completed, cancelled, or returned. |
| `source_update_ts` | Internal | Non-PII | Technical lineage timestamp when present. |
| `record_hash` | Internal | Non-PII | Technical deduplication value when present. |

## Product Data

Product data is usually **Internal** because it describes catalog and commercial attributes, not people.

| Column | Classification | PII | Notes |
| --- | --- | --- | --- |
| `product_id` | Internal | Non-PII | Product identifier. |
| `product_name` | Internal | Non-PII | Catalog label. |
| `category` | Internal | Non-PII | Catalog grouping. |
| `unit_cost` | Internal | Non-PII | Commercially sensitive cost field. Do not expose publicly. |

## Masking Notes

The repo includes `src/privacy.py` with simple helpers for customer fields:

- `mask_email("alice@example.com")` returns `a***@example.com`.
- `mask_name("Alice Johnson")` returns `A*** J***`.
- `mask_customer_columns(dataframe)` masks customer names and emails before Spark display or logging.

Use the masked representation for logs, dashboards, screenshots, support output, and any future examples that include `customer_name` or `email`. Keep raw values only in controlled tables where the pipeline actually requires them.

## Secret Handling

- Commit `.env.example`; never commit `.env`.
- Prefer environment variables for credentials such as `AWS_SECRET_ACCESS_KEY` and `MINIO_ROOT_PASSWORD`.
- Run `make security` before sharing changes when security-sensitive files changed.
- Use synthetic fixtures only. Do not add real customer, order, or credential data to `seed_data/`, `tests/fixtures/`, or docs.
