from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

from src.config import load_app_config, table_path

config = load_app_config()

builder = (
    SparkSession.builder.appName("EcommerceLakehouseSmokeTest")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

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
