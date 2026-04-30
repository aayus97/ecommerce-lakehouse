import duckdb


def query_delta_dir(connection, table_path, select_sql, order_by=None):
    query = f"{select_sql} FROM read_parquet('{table_path}/**/*.parquet')"
    if order_by:
        query = f"{query} ORDER BY {order_by}"

    return connection.execute(query).fetchall()


def test_mini_pipeline_creates_bronze_silver_and_gold_outputs(mini_pipeline_outputs):
    connection = duckdb.connect()

    assert (
        query_delta_dir(
            connection,
            mini_pipeline_outputs["bronze_orders"],
            "SELECT count(*)",
        )[0][0]
        == 6
    )
    assert (
        query_delta_dir(
            connection,
            mini_pipeline_outputs["orders_validated"],
            "SELECT count(*)",
        )[0][0]
        == 6
    )

    silver_summary = query_delta_dir(
        connection,
        mini_pipeline_outputs["orders_silver"],
        "SELECT count(*), count(DISTINCT status), min(status), round(sum(total_amount), 2)",
    )[0]
    assert silver_summary == (4, 1, "completed", 256.5)

    daily_sales = query_delta_dir(
        connection,
        mini_pipeline_outputs["daily_sales_summary"],
        (
            "SELECT CAST(order_date AS VARCHAR), total_orders, unique_customers, "
            "round(daily_revenue, 2)"
        ),
        order_by="order_date",
    )
    assert daily_sales == [
        ("2026-01-01", 2, 2, 171.0),
        ("2026-01-02", 1, 1, 25.5),
        ("2026-01-03", 1, 1, 60.0),
    ]


def test_mini_pipeline_joins_dimensions_into_category_country_gold(
    mini_pipeline_outputs,
):
    connection = duckdb.connect()

    category_country = query_delta_dir(
        connection,
        mini_pipeline_outputs["revenue_by_category_country"],
        ("SELECT country, category, total_orders, unique_customers, round(revenue, 2)"),
        order_by="country, category",
    )

    assert category_country == [
        ("France", "Electronics", 2, 2, 76.5),
        ("Germany", "Electronics", 1, 1, 120.0),
        ("Germany", "Home Office", 1, 1, 60.0),
    ]
