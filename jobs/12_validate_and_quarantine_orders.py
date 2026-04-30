import os

from src.config import load_app_config, path_value, table_path
from src.logger import get_logger
from src.metrics import write_metric
from src.order_validation import validate_orders, write_validation_summary
from src.spark_session import get_spark

config = load_app_config()
logger = get_logger("validate_orders")

spark = get_spark("ValidateAndQuarantineOrders")

orders_bronze_path = table_path(config, "orders_bronze")
orders_quarantine_path = table_path(config, "orders_quarantine")
orders_validated_path = table_path(config, "orders_validated")
quality_config = config.get("data_quality", {})
invalid_percentage_threshold = quality_config.get("invalid_percentage_threshold", 5.0)
summary_path = quality_config.get(
    "orders_validation_summary_path",
    f"{path_value(config, 'metrics')}/orders_validation_summary.json",
)

orders = spark.read.format("delta").load(orders_bronze_path)

validation_result = validate_orders(
    orders,
    invalid_percentage_threshold=invalid_percentage_threshold,
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

if invalid_count > 0:
    invalid_orders.write.format("delta").mode("overwrite").save(orders_quarantine_path)
    print(f"Invalid rows written to {orders_quarantine_path}")

valid_orders.write.format("delta").mode("overwrite").save(orders_validated_path)
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
        "invalid_rows": invalid_count,
        "invalid_percentage": invalid_percentage,
        "invalid_percentage_threshold": invalid_percentage_threshold,
        "threshold_passed": summary["threshold_passed"],
        "quarantine_reason_counts": summary["quarantine_reason_counts"],
        "all_quarantine_reason_counts": summary["all_quarantine_reason_counts"],
        "summary_path": summary_path,
        "quarantine_path": orders_quarantine_path,
        "validated_path": orders_validated_path,
    },
)

if not summary["threshold_passed"]:
    spark.stop()
    raise Exception(
        "Orders data quality failed: invalid row percentage "
        f"{invalid_percentage}% exceeds threshold {invalid_percentage_threshold}%"
    )

spark.stop()
