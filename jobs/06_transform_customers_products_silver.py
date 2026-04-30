from pyspark.sql.functions import col, to_date, lower, trim

from src.config import load_app_config, table_path
from src.spark_session import get_spark

config = load_app_config()
spark = get_spark("TransformCustomersProductsSilver")

customers = spark.read.format("delta").load(table_path(config, "customers_bronze"))

customers_silver = (
    customers.withColumn("customer_name", trim(col("customer_name")))
    .withColumn("email", lower(trim(col("email"))))
    .withColumn("country", trim(col("country")))
    .withColumn("signup_date", to_date(col("signup_date")))
)

customers_silver.write.format("delta").mode("overwrite").save(
    table_path(config, "customers_silver")
)


products = spark.read.format("delta").load(table_path(config, "products_bronze"))

products_silver = (
    products.withColumn("product_name", trim(col("product_name")))
    .withColumn("category", trim(col("category")))
    .withColumn("unit_cost", col("unit_cost").cast("double"))
)

products_silver.write.format("delta").mode("overwrite").save(
    table_path(config, "products_silver")
)

print("Silver customers:")
spark.read.format("delta").load(table_path(config, "customers_silver")).show()

print("Silver products:")
spark.read.format("delta").load(table_path(config, "products_silver")).show()

spark.stop()
