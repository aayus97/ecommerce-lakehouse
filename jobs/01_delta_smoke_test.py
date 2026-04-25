from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

builder = (
    SparkSession.builder
    .appName("EcommerceLakehouseSmokeTest")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

data = [
    (1, "Alice", 120.50),
    (2, "Bob", 75.00),
    (3, "Charlie", 220.99),
]

df = spark.createDataFrame(data, ["order_id", "customer_name", "amount"])

df.write.format("delta").mode("overwrite").save("data/bronze/orders")

result = spark.read.format("delta").load("data/bronze/orders")
result.show()

spark.stop()
