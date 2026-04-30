from pathlib import Path

import duckdb
import pytest
from pyspark.sql import SparkSession

from src.order_validation import schema_validation_errors

FIXTURES = Path(__file__).resolve().parent / "fixtures"

RAW_CONTRACTS = {
    "orders_valid.csv": {
        "order_id": "int",
        "customer_id": "int",
        "product_id": "int",
        "order_date": "date",
        "quantity": "int",
        "unit_price": "double",
        "status": "string",
    },
    "customers.csv": {
        "customer_id": "int",
        "customer_name": "string",
        "email": "string",
        "country": "string",
        "signup_date": "date",
    },
    "products.csv": {
        "product_id": "int",
        "product_name": "string",
        "category": "string",
        "unit_cost": "double",
    },
}

GOLD_CONTRACTS = {
    "daily_sales_summary": {
        "order_date": "DATE",
        "total_orders": "BIGINT",
        "unique_customers": "BIGINT",
        "daily_revenue": "DOUBLE",
    },
    "revenue_by_category_country": {
        "country": "VARCHAR",
        "category": "VARCHAR",
        "total_orders": "BIGINT",
        "unique_customers": "BIGINT",
        "revenue": "DOUBLE",
    },
}


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.appName("SchemaContractTests")
        .master("local[1]")
        .getOrCreate()
    )
    yield session
    session.stop()


def read_csv_schema(spark, file_name):
    return (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(str(FIXTURES / file_name))
        .schema
    )


def test_raw_fixture_schemas_match_contracts(spark):
    for file_name, expected_schema in RAW_CONTRACTS.items():
        schema = read_csv_schema(spark, file_name)

        actual_schema = {
            field.name: field.dataType.simpleString() for field in schema.fields
        }

        assert actual_schema == expected_schema


def test_valid_order_schema_satisfies_validation_contract(spark):
    orders = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(str(FIXTURES / "orders_valid.csv"))
    )

    assert schema_validation_errors(orders) == []


def test_invalid_order_schema_fails_contract_when_required_column_is_missing(spark):
    orders = spark.createDataFrame(
        [
            {
                "order_id": 1,
                "customer_id": 101,
                "order_date": "2026-01-01",
                "quantity": 1,
                "unit_price": 10.0,
                "status": "completed",
            }
        ]
    )

    errors = schema_validation_errors(orders)

    assert any(
        error["column"] == "product_id" and error["reason"] == "missing_required_column"
        for error in errors
    )


def test_gold_table_schemas_match_contracts(mini_pipeline_outputs):
    connection = duckdb.connect()

    for table_name, expected_schema in GOLD_CONTRACTS.items():
        table_path = mini_pipeline_outputs[table_name]
        describe_rows = connection.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{table_path}/**/*.parquet')"
        ).fetchall()
        actual_schema = {row[0]: row[1] for row in describe_rows}

        assert actual_schema == expected_schema
