from src.config import load_app_config, table_path
from src.spark_session import get_spark

config = load_app_config()

spark = get_spark("EcommerceLakehouseSmokeTest")

data = [
    (1, "Alice", 120.50),
    (2, "Bob", 75.00),
    (3, "Charlie", 220.99),
]

df = spark.createDataFrame(data, ["order_id", "customer_name", "amount"])

df.write.format("delta").mode("overwrite").save(table_path(config, "orders_bronze"))

result = spark.read.format("delta").load(table_path(config, "orders_bronze"))
result.show()

spark.stop()
