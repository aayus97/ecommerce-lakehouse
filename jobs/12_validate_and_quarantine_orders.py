from pyspark.sql.functions import col
from src.spark_session import get_spark

spark = get_spark("ValidateAndQuarantineOrders")

orders = spark.read.format("delta").load("data/bronze/orders")

valid_statuses = ["completed", "cancelled", "returned"]

invalid_condition = (
    col("order_id").isNull()
    | col("customer_id").isNull()
    | col("product_id").isNull()
    | col("quantity").isNull()
    | col("unit_price").isNull()
    | (col("quantity") <= 0)
    | (col("unit_price") < 0)
    | (~col("status").isin(valid_statuses))
)

invalid_orders = orders.filter(invalid_condition)
valid_orders = orders.filter(~invalid_condition)

invalid_count = invalid_orders.count()
valid_count = valid_orders.count()

print(f"Valid rows: {valid_count}")
print(f"Invalid rows: {invalid_count}")

if invalid_count > 0:
    invalid_orders.write.format("delta").mode("overwrite").save("data/quarantine/orders")
    print("Invalid rows written to data/quarantine/orders")

valid_orders.write.format("delta").mode("overwrite").save("data/bronze/orders_validated")
print("Valid rows written to data/bronze/orders_validated")

spark.stop()