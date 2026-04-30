from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
import json

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    array,
    col,
    count,
    current_date,
    element_at,
    explode,
    lit,
    size,
    to_date,
    when,
)
from pyspark.sql.types import (
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
    TimestampType,
)
from pyspark.sql.window import Window

VALID_STATUSES = ("completed", "cancelled", "returned")
VALIDATION_REASON_COLUMN = "quarantine_reason"
VALIDATION_REASONS_COLUMN = "quarantine_reasons"

EXPECTED_ORDER_COLUMNS = {
    "order_id": (IntegerType, LongType, StringType),
    "customer_id": (IntegerType, LongType, StringType),
    "product_id": (IntegerType, LongType, StringType),
    "order_date": (DateType, TimestampType, StringType),
    "quantity": (IntegerType, LongType, ShortType, DoubleType, FloatType, DecimalType),
    "unit_price": (
        IntegerType,
        LongType,
        ShortType,
        DoubleType,
        FloatType,
        DecimalType,
    ),
    "status": (StringType,),
}

ORDER_EXPECTATIONS = (
    {
        "name": "required_order_columns_present",
        "level": "schema",
        "reason": "missing_required_column",
    },
    {
        "name": "order_id_unique",
        "level": "uniqueness",
        "reason": "duplicate_order_id",
    },
    {
        "name": "order_id_not_null",
        "level": "null",
        "reason": "missing_order_id",
    },
    {
        "name": "customer_id_not_null",
        "level": "null",
        "reason": "missing_customer_id",
    },
    {
        "name": "product_id_not_null",
        "level": "null",
        "reason": "missing_product_id",
    },
    {
        "name": "quantity_positive",
        "level": "range",
        "reason": "invalid_quantity",
    },
    {
        "name": "unit_price_non_negative",
        "level": "range",
        "reason": "invalid_unit_price",
    },
    {
        "name": "status_allowed",
        "level": "business_rule",
        "reason": "invalid_status",
    },
    {
        "name": "order_date_not_future",
        "level": "business_rule",
        "reason": "future_order_date",
    },
    {
        "name": "customer_id_exists",
        "level": "referential",
        "reason": "unknown_customer_id",
    },
    {
        "name": "product_id_exists",
        "level": "referential",
        "reason": "unknown_product_id",
    },
    {
        "name": "invalid_row_percentage_below_threshold",
        "level": "business_rule",
        "reason": "invalid_percentage_threshold_exceeded",
    },
)


@dataclass
class OrderValidationResult:
    valid_rows: DataFrame
    quarantined_rows: DataFrame
    summary: dict


def invalid_order_condition():
    return (
        col("order_id").isNull()
        | col("customer_id").isNull()
        | col("product_id").isNull()
        | col("quantity").isNull()
        | col("unit_price").isNull()
        | (col("quantity") <= 0)
        | (col("unit_price") < 0)
        | col("status").isNull()
        | (~col("status").isin(*VALID_STATUSES))
        | to_date(col("order_date")).isNull()
        | (to_date(col("order_date")) > current_date())
    )


def order_quarantine_reasons(order: dict, today: date | None = None) -> list[str]:
    today = today or date.today()
    reasons = []

    if order.get("order_id") is None:
        reasons.append("missing_order_id")

    if order.get("customer_id") is None:
        reasons.append("missing_customer_id")

    if order.get("product_id") is None:
        reasons.append("missing_product_id")

    if order.get("quantity") is None or order["quantity"] <= 0:
        reasons.append("invalid_quantity")

    if order.get("unit_price") is None or order["unit_price"] < 0:
        reasons.append("invalid_unit_price")

    if order.get("status") not in VALID_STATUSES:
        reasons.append("invalid_status")

    order_date = _coerce_date(order.get("order_date"))
    if order.get("order_date") is not None and order_date is None:
        reasons.append("future_order_date")
    elif order_date is not None and order_date > today:
        reasons.append("future_order_date")

    return reasons


def _coerce_date(value):
    if isinstance(value, datetime):
        return value.date()

    if value is None or isinstance(value, date):
        return value

    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError:
            return None

    return None


def is_valid_order(order: dict) -> bool:
    return not order_quarantine_reasons(order)


def schema_validation_errors(orders: DataFrame) -> list[dict]:
    errors = []
    columns = set(orders.columns)

    for column_name, allowed_types in EXPECTED_ORDER_COLUMNS.items():
        if column_name not in columns:
            errors.append(
                {
                    "level": "schema",
                    "column": column_name,
                    "reason": "missing_required_column",
                    "message": f"Missing required column: {column_name}",
                }
            )
            continue

        actual_type = orders.schema[column_name].dataType
        if not isinstance(actual_type, allowed_types):
            allowed_type_names = [data_type.__name__ for data_type in allowed_types]
            errors.append(
                {
                    "level": "schema",
                    "column": column_name,
                    "reason": "invalid_column_type",
                    "message": (
                        f"Column {column_name} has type {actual_type.simpleString()}, "
                        f"expected one of {allowed_type_names}"
                    ),
                }
            )

    return errors


def _with_missing_columns(orders: DataFrame) -> DataFrame:
    normalized = orders

    for column_name in EXPECTED_ORDER_COLUMNS:
        if column_name not in normalized.columns:
            normalized = normalized.withColumn(column_name, lit(None))

    return normalized


def with_order_validation_columns(
    orders: DataFrame,
    today_column=None,
    customer_ids: DataFrame | None = None,
    product_ids: DataFrame | None = None,
) -> DataFrame:
    if today_column is None:
        today_column = current_date()
    validated = _with_missing_columns(orders)
    order_date = to_date(col("order_date"))

    duplicate_window = Window.partitionBy("order_id")
    validated = validated.withColumn(
        "_duplicate_order_id_count",
        count("*").over(duplicate_window),
    )

    reason_columns = [
        when(col("order_id").isNull(), lit("missing_order_id")),
        when(col("customer_id").isNull(), lit("missing_customer_id")),
        when(col("product_id").isNull(), lit("missing_product_id")),
        when(
            col("quantity").isNull() | (col("quantity") <= 0), lit("invalid_quantity")
        ),
        when(
            col("unit_price").isNull() | (col("unit_price") < 0),
            lit("invalid_unit_price"),
        ),
        when(
            col("status").isNull() | ~col("status").isin(*VALID_STATUSES),
            lit("invalid_status"),
        ),
        when(
            order_date.isNull() | (order_date > today_column), lit("future_order_date")
        ),
        when(
            col("order_id").isNotNull() & (col("_duplicate_order_id_count") > 1),
            lit("duplicate_order_id"),
        ),
    ]

    validated = validated.withColumn(
        VALIDATION_REASONS_COLUMN,
        array(*reason_columns),
    ).withColumn(
        VALIDATION_REASONS_COLUMN,
        expr_filter_not_null(VALIDATION_REASONS_COLUMN),
    )

    if customer_ids is not None:
        validated = validated.join(
            customer_ids.select("customer_id")
            .distinct()
            .withColumn("_known_customer", lit(True)),
            on="customer_id",
            how="left",
        )
        validated = _append_reason(
            validated,
            col("customer_id").isNotNull() & col("_known_customer").isNull(),
            "unknown_customer_id",
        ).drop("_known_customer")

    if product_ids is not None:
        validated = validated.join(
            product_ids.select("product_id")
            .distinct()
            .withColumn("_known_product", lit(True)),
            on="product_id",
            how="left",
        )
        validated = _append_reason(
            validated,
            col("product_id").isNotNull() & col("_known_product").isNull(),
            "unknown_product_id",
        ).drop("_known_product")

    return validated.withColumn(
        VALIDATION_REASON_COLUMN,
        first_reason_column(),
    ).drop("_duplicate_order_id_count")


def expr_filter_not_null(column_name: str):
    from pyspark.sql.functions import expr

    return expr(f"filter({column_name}, reason -> reason is not null)")


def _append_reason(orders: DataFrame, condition, reason: str) -> DataFrame:
    from pyspark.sql.functions import concat

    return orders.withColumn(
        VALIDATION_REASONS_COLUMN,
        when(
            condition,
            concat(col(VALIDATION_REASONS_COLUMN), array(lit(reason))),
        ).otherwise(col(VALIDATION_REASONS_COLUMN)),
    ).withColumn(
        VALIDATION_REASON_COLUMN,
        first_reason_column(),
    )


def first_reason_column():
    return when(
        size(col(VALIDATION_REASONS_COLUMN)) > 0,
        element_at(col(VALIDATION_REASONS_COLUMN), 1),
    ).otherwise(lit(None))


def validate_orders(
    orders: DataFrame,
    invalid_percentage_threshold: float = 5.0,
    today_column=None,
    customer_ids: DataFrame | None = None,
    product_ids: DataFrame | None = None,
) -> OrderValidationResult:
    schema_errors = schema_validation_errors(orders)
    validated = with_order_validation_columns(
        orders,
        today_column=today_column,
        customer_ids=customer_ids,
        product_ids=product_ids,
    )

    quarantined_rows = validated.filter(col(VALIDATION_REASON_COLUMN).isNotNull())
    valid_rows = validated.filter(col(VALIDATION_REASON_COLUMN).isNull()).drop(
        VALIDATION_REASON_COLUMN,
        VALIDATION_REASONS_COLUMN,
    )

    summary = build_validation_summary(
        total_rows=validated.count(),
        valid_rows=valid_rows.count(),
        quarantined_rows=quarantined_rows,
        schema_errors=schema_errors,
        invalid_percentage_threshold=invalid_percentage_threshold,
    )

    return OrderValidationResult(
        valid_rows=valid_rows,
        quarantined_rows=quarantined_rows,
        summary=summary,
    )


def build_validation_summary(
    total_rows: int,
    valid_rows: int,
    quarantined_rows: DataFrame,
    schema_errors: list[dict],
    invalid_percentage_threshold: float,
) -> dict:
    invalid_rows = quarantined_rows.count()
    invalid_percentage = (
        round((invalid_rows / total_rows) * 100, 2) if total_rows else 0
    )
    reason_counts = {
        row[VALIDATION_REASON_COLUMN]: row["count"]
        for row in quarantined_rows.groupBy(VALIDATION_REASON_COLUMN).count().collect()
    }
    all_reason_counts = {
        row["reason"]: row["count"]
        for row in quarantined_rows.select(
            explode(col(VALIDATION_REASONS_COLUMN)).alias("reason")
        )
        .groupBy("reason")
        .count()
        .collect()
    }

    return {
        "expectations": list(ORDER_EXPECTATIONS),
        "levels": {
            "schema": {"passed": not schema_errors, "errors": schema_errors},
            "null": {
                "reasons": [
                    "missing_order_id",
                    "missing_customer_id",
                    "missing_product_id",
                ]
            },
            "uniqueness": {"reasons": ["duplicate_order_id"]},
            "range": {"reasons": ["invalid_quantity", "invalid_unit_price"]},
            "referential": {"reasons": ["unknown_customer_id", "unknown_product_id"]},
            "business_rule": {"reasons": ["invalid_status", "future_order_date"]},
        },
        "total_rows": total_rows,
        "valid_rows": valid_rows,
        "invalid_rows": invalid_rows,
        "invalid_percentage": invalid_percentage,
        "invalid_percentage_threshold": invalid_percentage_threshold,
        "threshold_passed": invalid_percentage <= invalid_percentage_threshold,
        "quarantine_reason_counts": reason_counts,
        "all_quarantine_reason_counts": all_reason_counts,
    }


def write_validation_summary(path: str, summary: dict):
    report_path = Path(path)
    report_path.parent.mkdir(parents=True, exist_ok=True)

    with report_path.open("w") as f:
        json.dump(summary, f, indent=2, sort_keys=True)
