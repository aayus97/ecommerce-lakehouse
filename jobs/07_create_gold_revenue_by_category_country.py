from pyspark.sql.functions import sum, count, countDistinct, round

from src.config import load_app_config, table_path
from src.spark_session import get_spark

config = load_app_config()
spark = get_spark("GoldRevenueByCategoryCountry")

orders = spark.read.format("delta").load(table_path(config, "orders_silver"))
customers = spark.read.format("delta").load(table_path(config, "customers_silver"))
products = spark.read.format("delta").load(table_path(config, "products_silver"))

joined = orders.join(customers, on="customer_id", how="left").join(
    products, on="product_id", how="left"
)

gold = (
    joined.groupBy("country", "category")
    .agg(
        count("order_id").alias("total_orders"),
        countDistinct("customer_id").alias("unique_customers"),
        round(sum("total_amount"), 2).alias("revenue"),
    )
    .orderBy("country", "category")
)

gold.write.format("delta").mode("overwrite").save(
    table_path(config, "revenue_by_category_country")
)

gold.show()

spark.stop()
