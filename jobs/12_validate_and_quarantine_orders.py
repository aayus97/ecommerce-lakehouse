import os
import time

from src.backfill import (
    affected_order_dates,
    backfill_metric_details,
    filter_by_order_date,
    is_backfill_run,
)
from src.config import load_app_config, path_value, table_path
from src.delta_utils import write_date_partitions_delta, write_partitioned_delta
from src.logger import get_logger
from src.metrics import write_metric, write_step_metric
from src.order_validation import validate_orders, write_validation_summary
from src.spark_session import get_spark

config = load_app_config()
logger = get_logger("validate_orders")

spark = get_spark("ValidateAndQuarantineOrders")
start_time = time.time()

orders_bronze_path = table_path(config, "orders_bronze")
customers_bronze_path = table_path(config, "customers_bronze")
products_bronze_path = table_path(config, "products_bronze")
orders_quarantine_path = table_path(config, "orders_quarantine")
orders_validated_path = table_path(config, "orders_validated")
quality_config = config.get("data_quality", {})
invalid_percentage_threshold = quality_config.get("invalid_percentage_threshold", 5.0)
summary_path = quality_config.get(
    "orders_validation_summary_path",
    f"{path_value(config, 'metrics')}/orders_validation_summary.json",
)

orders = filter_by_order_date(spark.read.format("delta").load(orders_bronze_path))
affected_dates = affected_order_dates(orders)
customer_ids = (
    spark.read.format("delta").load(customers_bronze_path).select("customer_id")
)
product_ids = spark.read.format("delta").load(products_bronze_path).select("product_id")

validation_result = validate_orders(
    orders,
    invalid_percentage_threshold=invalid_percentage_threshold,
    customer_ids=customer_ids,
    product_ids=product_ids,
)

invalid_orders = validation_result.quarantined_rows
valid_orders = validation_result.valid_rows
summary = validation_result.summary

invalid_count = summary["invalid_rows"]
valid_count = summary["valid_rows"]
total_count = summary["total_rows"]
invalid_percentage = summary["invalid_percentage"]

logger.info(f"Valid rows: {valid_count}")
logger.info(f"Invalid rows: {invalid_count}")
logger.info(f"Invalid row percentage: {invalid_percentage}%")

if is_backfill_run():
    valid_write_mode = write_date_partitions_delta(
        spark,
        valid_orders,
        orders_validated_path,
        affected_dates,
    )
    quarantine_write_mode = write_date_partitions_delta(
        spark,
        invalid_orders,
        orders_quarantine_path,
        affected_dates,
    )
elif invalid_count > 0:
    write_partitioned_delta(invalid_orders, orders_quarantine_path)
    valid_write_mode = "overwrite"
    quarantine_write_mode = "overwrite"
    print(f"Invalid rows written to {orders_quarantine_path}")
else:
    valid_write_mode = "overwrite"
    quarantine_write_mode = "skipped"

if not is_backfill_run():
    write_partitioned_delta(valid_orders, orders_validated_path)
print(f"Valid rows written to {orders_validated_path}")

write_validation_summary(summary_path, summary)
print(f"Data quality summary written to {summary_path}")

pipeline_name = os.environ.get("PIPELINE_NAME")
run_id = os.environ.get("PIPELINE_RUN_ID")

write_metric(
    "orders_data_quality",
    {
        "pipeline_name": pipeline_name,
        "run_id": run_id,
        "total_rows": total_count,
        "valid_rows": valid_count,
        "invalid_count": invalid_count,
        "invalid_rows": invalid_count,
        "invalid_percentage": invalid_percentage,
        "rule_failure_counts": summary["quarantine_reason_counts"],
        "invalid_percentage_threshold": invalid_percentage_threshold,
        "threshold_passed": summary["threshold_passed"],
        "quarantine_reason_counts": summary["quarantine_reason_counts"],
        "all_quarantine_reason_counts": summary["all_quarantine_reason_counts"],
        "summary_path": summary_path,
        "quarantine_path": orders_quarantine_path,
        "validated_path": orders_validated_path,
        "affected_dates": affected_dates,
        **backfill_metric_details(),
    },
)

write_step_metric(
    "validate_orders",
    rows_read=total_count,
    rows_written=valid_count,
    rows_quarantined=invalid_count,
    duration_seconds=round(time.time() - start_time, 2),
    input_path=[orders_bronze_path, customers_bronze_path, products_bronze_path],
    output_path=orders_validated_path,
    status="success" if summary["threshold_passed"] else "failed",
    details={
        "partition_columns": ["order_date"],
        "quarantine_path": orders_quarantine_path,
        "affected_dates": affected_dates,
        "valid_write_mode": valid_write_mode,
        "quarantine_write_mode": quarantine_write_mode,
        **backfill_metric_details(),
    },
)

if not summary["threshold_passed"]:
    spark.stop()
    raise Exception(
        "Orders data quality failed: invalid row percentage "
        f"{invalid_percentage}% exceeds threshold {invalid_percentage_threshold}%"
    )

spark.stop()
