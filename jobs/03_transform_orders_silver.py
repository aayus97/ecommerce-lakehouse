from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, round
from delta import configure_spark_with_delta_pip

from src.config import load_app_config, table_path

config = load_app_config()

builder = (
    SparkSession.builder.appName("TransformOrdersSilver")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

bronze = spark.read.format("delta").load(table_path(config, "orders_validated"))

silver = (
    bronze.withColumn("order_date", to_date(col("order_date")))
    .withColumn("quantity", col("quantity").cast("int"))
    .withColumn("unit_price", col("unit_price").cast("double"))
    .withColumn("total_amount", round(col("quantity") * col("unit_price"), 2))
    .filter(col("status") == "completed")
)

silver.write.format("delta").mode("overwrite").save(table_path(config, "orders_silver"))

print("Silver orders table created")
spark.read.format("delta").load(table_path(config, "orders_silver")).show()

spark.stop()
