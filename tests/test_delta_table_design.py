from src.delta_utils import prepare_dimension_for_scd2, prepare_orders_for_upsert


def test_order_upsert_preparation_keeps_latest_update_by_order_id(spark):
    orders = spark.createDataFrame(
        [
            {
                "order_id": 1,
                "customer_id": 101,
                "product_id": 1001,
                "order_date": "2026-01-01",
                "quantity": 1,
                "unit_price": 10.0,
                "status": "completed",
                "update_timestamp": "2026-01-01 08:00:00",
            },
            {
                "order_id": 1,
                "customer_id": 101,
                "product_id": 1001,
                "order_date": "2026-01-01",
                "quantity": 2,
                "unit_price": 10.0,
                "status": "completed",
                "update_timestamp": "2026-01-01 09:00:00",
            },
            {
                "order_id": 2,
                "customer_id": 102,
                "product_id": 1002,
                "order_date": "2026-01-02",
                "quantity": 1,
                "unit_price": 20.0,
                "status": "completed",
                "update_timestamp": "2026-01-01 07:00:00",
            },
        ]
    )

    prepared = prepare_orders_for_upsert(orders)
    rows = {
        row["order_id"]: row
        for row in prepared.select(
            "order_id",
            "quantity",
            "source_update_ts",
            "ingestion_date",
            "record_hash",
        ).collect()
    }

    assert set(rows) == {1, 2}
    assert rows[1]["quantity"] == 2
    assert rows[1]["source_update_ts"].isoformat() == "2026-01-01T09:00:00"
    assert rows[1]["ingestion_date"] is not None
    assert rows[1]["record_hash"]


def test_dimension_scd2_preparation_keeps_latest_version_by_business_key(spark):
    customers = spark.createDataFrame(
        [
            {
                "customer_id": 101,
                "customer_name": "Alice",
                "email": "alice@example.com",
                "country": "France",
                "signup_date": "2026-01-01",
                "update_timestamp": "2026-01-02 08:00:00",
            },
            {
                "customer_id": 101,
                "customer_name": "Alice",
                "email": "alice@example.com",
                "country": "Germany",
                "signup_date": "2026-01-01",
                "update_timestamp": "2026-01-03 08:00:00",
            },
            {
                "customer_id": 102,
                "customer_name": "Bob",
                "email": "bob@example.com",
                "country": "France",
                "signup_date": "2026-01-02",
                "update_timestamp": "2026-01-02 09:00:00",
            },
        ]
    )

    prepared = prepare_dimension_for_scd2(
        customers,
        key_column="customer_id",
        business_columns=[
            "customer_id",
            "customer_name",
            "email",
            "country",
            "signup_date",
        ],
    )
    rows = {
        row["customer_id"]: row
        for row in prepared.select(
            "customer_id",
            "country",
            "source_update_ts",
            "valid_from",
            "valid_to",
            "is_current",
            "record_hash",
        ).collect()
    }

    assert set(rows) == {101, 102}
    assert rows[101]["country"] == "Germany"
    assert rows[101]["source_update_ts"].isoformat() == "2026-01-03T08:00:00"
    assert rows[101]["valid_from"] == rows[101]["source_update_ts"]
    assert rows[101]["valid_to"] is None
    assert rows[101]["is_current"] is True
    assert rows[101]["record_hash"]


def test_orders_tables_are_partitioned_by_order_date(mini_pipeline_outputs):
    for table_key in ("bronze_orders", "orders_validated", "orders_silver"):
        table_path = mini_pipeline_outputs[table_key]

        assert any(table_path.glob("order_date=*"))


def test_gold_tables_are_partitioned_by_order_date(mini_pipeline_outputs):
    for table_key in ("daily_sales_summary", "revenue_by_category_country"):
        table_path = mini_pipeline_outputs[table_key]

        assert any(table_path.glob("order_date=*"))
