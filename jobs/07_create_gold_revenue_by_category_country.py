from pyspark.sql.functions import sum, count, countDistinct, round
from src.spark_session import get_spark

spark = get_spark("GoldRevenueByCategoryCountry")

orders = spark.read.format("delta").load("data/silver/orders")
customers = spark.read.format("delta").load("data/silver/customers")
products = spark.read.format("delta").load("data/silver/products")

joined = (
    orders
    .join(customers, on="customer_id", how="left")
    .join(products, on="product_id", how="left")
)

gold = (
    joined
    .groupBy("country", "category")
    .agg(
        count("order_id").alias("total_orders"),
        countDistinct("customer_id").alias("unique_customers"),
        round(sum("total_amount"), 2).alias("revenue")
    )
    .orderBy("country", "category")
)

gold.write.format("delta").mode("overwrite").save("data/gold/revenue_by_category_country")

gold.show()

spark.stop()