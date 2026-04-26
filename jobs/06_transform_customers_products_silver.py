from pyspark.sql.functions import col, to_date, lower, trim
from src.spark_session import get_spark

spark = get_spark("TransformCustomersProductsSilver")

customers = spark.read.format("delta").load("data/bronze/customers")

customers_silver = (
    customers
    .withColumn("customer_name", trim(col("customer_name")))
    .withColumn("email", lower(trim(col("email"))))
    .withColumn("country", trim(col("country")))
    .withColumn("signup_date", to_date(col("signup_date")))
)

customers_silver.write.format("delta").mode("overwrite").save("data/silver/customers")


products = spark.read.format("delta").load("data/bronze/products")

products_silver = (
    products
    .withColumn("product_name", trim(col("product_name")))
    .withColumn("category", trim(col("category")))
    .withColumn("unit_cost", col("unit_cost").cast("double"))
)

products_silver.write.format("delta").mode("overwrite").save("data/silver/products")

print("Silver customers:")
spark.read.format("delta").load("data/silver/customers").show()

print("Silver products:")
spark.read.format("delta").load("data/silver/products").show()

spark.stop()