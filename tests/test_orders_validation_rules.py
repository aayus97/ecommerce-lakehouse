from datetime import date

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, to_date

from src.order_validation import (
    VALID_STATUSES,
    order_quarantine_reasons,
    is_valid_order,
    validate_orders,
)


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.appName("OrderValidationRulesTest")
        .master("local[1]")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_valid_order_passes():
    order = {
        "order_id": "o1",
        "customer_id": "c1",
        "product_id": "p1",
        "quantity": 2,
        "unit_price": 10.0,
        "status": "completed",
    }

    assert is_valid_order(order) is True


def test_negative_quantity_fails():
    order = {
        "order_id": "o1",
        "customer_id": "c1",
        "product_id": "p1",
        "quantity": -1,
        "unit_price": 10.0,
        "status": "completed",
    }

    assert is_valid_order(order) is False
    assert order_quarantine_reasons(order) == ["invalid_quantity"]


def test_invalid_status_fails():
    order = {
        "order_id": "o1",
        "customer_id": "c1",
        "product_id": "p1",
        "quantity": 1,
        "unit_price": 10.0,
        "status": "pending",
    }

    assert is_valid_order(order) is False
    assert order_quarantine_reasons(order) == ["invalid_status"]


def test_zero_quantity_fails():
    order = {
        "order_id": "o1",
        "customer_id": "c1",
        "product_id": "p1",
        "quantity": 0,
        "unit_price": 10.0,
        "status": "completed",
    }

    assert is_valid_order(order) is False


def test_negative_unit_price_fails():
    order = {
        "order_id": "o1",
        "customer_id": "c1",
        "product_id": "p1",
        "quantity": 1,
        "unit_price": -0.01,
        "status": "completed",
    }

    assert is_valid_order(order) is False
    assert order_quarantine_reasons(order) == ["invalid_unit_price"]


def test_future_order_date_fails():
    order = {
        "order_id": "o1",
        "customer_id": "c1",
        "product_id": "p1",
        "order_date": date(2026, 1, 11),
        "quantity": 1,
        "unit_price": 10.0,
        "status": "completed",
    }

    assert order_quarantine_reasons(order, today=date(2026, 1, 10)) == [
        "future_order_date"
    ]


def test_all_expected_statuses_are_valid():
    for status in VALID_STATUSES:
        order = {
            "order_id": "o1",
            "customer_id": "c1",
            "product_id": "p1",
            "quantity": 1,
            "unit_price": 10.0,
            "status": status,
        }

        assert is_valid_order(order) is True


def test_validate_orders_adds_quarantine_reasons_and_summary(spark):
    orders = spark.createDataFrame(
        [
            {
                "order_id": "o1",
                "customer_id": "c1",
                "product_id": "p1",
                "order_date": "2026-01-01",
                "quantity": 2,
                "unit_price": 10.0,
                "status": "completed",
            },
            {
                "order_id": "o2",
                "customer_id": "c1",
                "product_id": "p1",
                "order_date": "2026-01-01",
                "quantity": -1,
                "unit_price": 10.0,
                "status": "completed",
            },
            {
                "order_id": "o3",
                "customer_id": None,
                "product_id": "p1",
                "order_date": "2026-01-01",
                "quantity": 1,
                "unit_price": 10.0,
                "status": "completed",
            },
            {
                "order_id": "o4",
                "customer_id": "c1",
                "product_id": "p1",
                "order_date": "2026-01-01",
                "quantity": 1,
                "unit_price": 10.0,
                "status": "pending",
            },
            {
                "order_id": "o5",
                "customer_id": "c1",
                "product_id": "p1",
                "order_date": "2026-01-11",
                "quantity": 1,
                "unit_price": 10.0,
                "status": "completed",
            },
            {
                "order_id": "dup",
                "customer_id": "c1",
                "product_id": "p1",
                "order_date": "2026-01-01",
                "quantity": 1,
                "unit_price": 10.0,
                "status": "completed",
            },
            {
                "order_id": "dup",
                "customer_id": "c1",
                "product_id": "p1",
                "order_date": "2026-01-01",
                "quantity": 1,
                "unit_price": 10.0,
                "status": "completed",
            },
        ]
    )

    result = validate_orders(
        orders,
        invalid_percentage_threshold=90.0,
        today_column=to_date(lit("2026-01-10")),
    )

    quarantined = {
        row["order_id"]: row["quarantine_reason"]
        for row in result.quarantined_rows.select(
            "order_id", "quarantine_reason"
        ).collect()
    }

    assert quarantined["o2"] == "invalid_quantity"
    assert quarantined["o3"] == "missing_customer_id"
    assert quarantined["o4"] == "invalid_status"
    assert quarantined["o5"] == "future_order_date"
    assert quarantined["dup"] == "duplicate_order_id"
    assert result.summary["invalid_rows"] == 6
    assert result.summary["valid_rows"] == 1
    assert result.summary["threshold_passed"] is True


def test_validate_orders_can_run_referential_checks(spark):
    orders = spark.createDataFrame(
        [
            {
                "order_id": "o1",
                "customer_id": "known_customer",
                "product_id": "known_product",
                "order_date": "2026-01-01",
                "quantity": 1,
                "unit_price": 10.0,
                "status": "completed",
            },
            {
                "order_id": "o2",
                "customer_id": "missing_customer",
                "product_id": "missing_product",
                "order_date": "2026-01-01",
                "quantity": 1,
                "unit_price": 10.0,
                "status": "completed",
            },
        ]
    )
    customers = spark.createDataFrame([{"customer_id": "known_customer"}])
    products = spark.createDataFrame([{"product_id": "known_product"}])

    result = validate_orders(
        orders,
        today_column=to_date(lit("2026-01-10")),
        customer_ids=customers,
        product_ids=products,
    )

    quarantined = result.quarantined_rows.select(
        "order_id",
        "quarantine_reason",
        "quarantine_reasons",
    ).collect()

    assert len(quarantined) == 1
    assert quarantined[0]["quarantine_reason"] == "unknown_customer_id"
    assert quarantined[0]["quarantine_reasons"] == [
        "unknown_customer_id",
        "unknown_product_id",
    ]
