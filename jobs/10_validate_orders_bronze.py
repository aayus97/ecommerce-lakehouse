from src.config import load_app_config, table_path
from src.order_validation import validate_orders
from src.spark_session import get_spark

config = load_app_config()
spark = get_spark("ValidateOrdersBronze")

orders = spark.read.format("delta").load(table_path(config, "orders_bronze"))

validation_result = validate_orders(orders)
invalid_orders = validation_result.quarantined_rows

invalid_count = validation_result.summary["invalid_rows"]

if invalid_count > 0:
    print(f"Data quality failed. Invalid rows: {invalid_count}")
    invalid_orders.show(truncate=False)
    raise Exception("Bronze orders validation failed")

print("Data quality passed for Bronze orders")

spark.stop()
