from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, countDistinct, count
from delta import configure_spark_with_delta_pip

builder = (
    SparkSession.builder
    .appName("GoldSalesSummary")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

silver = spark.read.format("delta").load("data/silver/orders")

gold = (
    silver
    .groupBy("order_date")
    .agg(
        count("order_id").alias("total_orders"),
        countDistinct("customer_id").alias("unique_customers"),
        sum("total_amount").alias("daily_revenue")
    )
)

gold.write.format("delta").mode("overwrite").save("data/gold/daily_sales_summary")

print("Gold daily sales summary created")
spark.read.format("delta").load("data/gold/daily_sales_summary").show()

spark.stop()