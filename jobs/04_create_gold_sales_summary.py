from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, countDistinct, count
from delta import configure_spark_with_delta_pip

from src.config import load_app_config, table_path

config = load_app_config()

builder = (
    SparkSession.builder.appName("GoldSalesSummary")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

silver = spark.read.format("delta").load(table_path(config, "orders_silver"))

gold = silver.groupBy("order_date").agg(
    count("order_id").alias("total_orders"),
    countDistinct("customer_id").alias("unique_customers"),
    sum("total_amount").alias("daily_revenue"),
)

gold.write.format("delta").mode("overwrite").save(
    table_path(config, "daily_sales_summary")
)

print("Gold daily sales summary created")
spark.read.format("delta").load(table_path(config, "daily_sales_summary")).show()

spark.stop()
