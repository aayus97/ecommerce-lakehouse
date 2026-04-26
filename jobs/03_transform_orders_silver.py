from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, round
from delta import configure_spark_with_delta_pip

builder = (
    SparkSession.builder
    .appName("TransformOrdersSilver")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# bronze = spark.read.format("delta").load("data/bronze/orders")
bronze = spark.read.format("delta").load("data/bronze/orders_validated")

silver = (
    bronze
    .withColumn("order_date", to_date(col("order_date")))
    .withColumn("quantity", col("quantity").cast("int"))
    .withColumn("unit_price", col("unit_price").cast("double"))
    .withColumn("total_amount", round(col("quantity") * col("unit_price"), 2))
    .filter(col("status") == "completed")
)

silver.write.format("delta").mode("overwrite").save("data/silver/orders")

print("Silver orders table created")
spark.read.format("delta").load("data/silver/orders").show()

spark.stop()