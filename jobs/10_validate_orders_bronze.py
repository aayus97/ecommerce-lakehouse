from pyspark.sql.functions import col
from src.spark_session import get_spark

spark = get_spark("ValidateOrdersBronze")

orders = spark.read.format("delta").load("data/bronze/orders")

valid_statuses = ["completed", "cancelled", "returned"]

invalid_orders = orders.filter(
    col("order_id").isNull()
    | col("customer_id").isNull()
    | col("product_id").isNull()
    | col("quantity").isNull()
    | col("unit_price").isNull()
    | (col("quantity") <= 0)
    | (col("unit_price") < 0)
    | (~col("status").isin(valid_statuses))
)

invalid_count = invalid_orders.count()

if invalid_count > 0:
    print(f"Data quality failed. Invalid rows: {invalid_count}")
    invalid_orders.show(truncate=False)
    raise Exception("Bronze orders validation failed")

print("Data quality passed for Bronze orders")

spark.stop()