from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

builder = (
    SparkSession.builder
    .appName("IngestOrdersBronze")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("data/raw/orders.csv")
)

df.write.format("delta").mode("overwrite").save("data/bronze/orders")

print("Bronze orders table created")
spark.read.format("delta").load("data/bronze/orders").show()

spark.stop()