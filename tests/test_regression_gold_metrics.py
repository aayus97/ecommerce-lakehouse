import json
from pathlib import Path

import duckdb

EXPECTED_METRICS_PATH = (
    Path(__file__).resolve().parent / "fixtures" / "expected_gold_metrics.json"
)


def rows_as_dicts(connection, table_path, select_sql, columns, order_by, group_by=None):
    query = f"{select_sql} FROM read_parquet('{table_path}/**/*.parquet')"
    if group_by:
        query = f"{query} GROUP BY {group_by}"
    query = f"{query} ORDER BY {order_by}"
    rows = connection.execute(query).fetchall()

    return [dict(zip(columns, row, strict=True)) for row in rows]


def test_gold_metrics_match_regression_fixture(mini_pipeline_outputs):
    connection = duckdb.connect()
    expected = json.loads(EXPECTED_METRICS_PATH.read_text())

    actual_daily_sales = rows_as_dicts(
        connection,
        mini_pipeline_outputs["daily_sales_summary"],
        (
            "SELECT CAST(order_date AS VARCHAR), total_orders, unique_customers, "
            "round(daily_revenue, 2)"
        ),
        ["order_date", "total_orders", "unique_customers", "daily_revenue"],
        "order_date",
    )
    actual_category_country = rows_as_dicts(
        connection,
        mini_pipeline_outputs["revenue_by_category_country"],
        (
            "SELECT country, category, sum(total_orders), "
            "sum(unique_customers), round(sum(revenue), 2)"
        ),
        ["country", "category", "total_orders", "unique_customers", "revenue"],
        "country, category",
        group_by="country, category",
    )

    assert actual_daily_sales == expected["daily_sales_summary"]
    assert actual_category_country == expected["revenue_by_category_country"]
