# Testing Strategy

The test suite is designed to prove the pipeline is repeatable, contract-safe,
and observable before it is presented as portfolio-grade work.

## Test Commands

Run the full suite in Docker:

```bash
make test
```

Run locally after installing dependencies:

```bash
python -m pytest -q
```

Run linting:

```bash
make lint
```

Run secret scanning:

```bash
make security
```

## Coverage Areas

| Area | Tests | What they protect |
| --- | --- | --- |
| Pipeline config | `tests/test_pipeline_config.py` | Required fields, duplicate step names, dependency existence, dependency order, cycles, retry values. |
| App config | `tests/test_app_config.py` | Environment config loading and table/path resolution. |
| Validation rules | `tests/test_orders_validation_rules.py` | Required fields, invalid ranges, invalid statuses, future dates, duplicates, referential checks, threshold behavior. |
| Table contracts | `tests/test_contract_table_schemas.py` | Raw fixture schemas, validation-compatible order schemas, gold table schemas. |
| Delta design | `tests/test_delta_table_design.py` | Latest-order upsert preparation and `order_date` partitioning. |
| Mini pipeline integration | `tests/test_integration_mini_pipeline.py` | End-to-end bronze, validated, silver, and gold outputs from deterministic fixtures. |
| Gold regression | `tests/test_regression_gold_metrics.py` | Aggregate outputs match checked-in expected metrics. |
| Core functions | `tests/test_unit_core_functions.py` | Utility behavior used by pipeline and observability code. |

## Test Data

Fixtures live in `tests/fixtures/` and are intentionally small. They include:

- valid and invalid orders;
- customer and product dimensions;
- expected gold metrics for regression checks.

Seed data for local pipeline runs lives in `seed_data/raw/` and is copied to
`data/raw` by `make seed-data` and `make pipeline`.

## Acceptance Criteria

A documentation or pipeline change is ready when:

- `python run_pipeline.py --validate-only` passes;
- `make test` or `python -m pytest -q` passes in the selected environment;
- contract tests pass for raw and gold schemas;
- validation tests still cover each quarantine reason;
- dashboard or metric changes are reflected in docs and screenshots;
- `make security` is run before sharing security-sensitive changes.

## Manual Verification

For changes that affect operations or dashboards:

1. Run `make pipeline`.
2. Launch `make dashboard`.
3. Confirm the latest run appears with success status.
4. Confirm row counts, quality metrics, freshness, and business metrics are
   populated.
5. If the dashboard layout changed, refresh screenshots under `docs/assets/`.
