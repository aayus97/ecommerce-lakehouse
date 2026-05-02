from pyspark.sql.functions import col, to_date, lower, trim
import time

from src.config import load_app_config, table_path
from src.delta_utils import merge_dimension_scd2
from src.metrics import write_step_metric
from src.privacy import mask_customer_columns
from src.spark_session import get_spark

config = load_app_config()
spark = get_spark("TransformCustomersProductsSilver")
start_time = time.time()
customers_bronze_path = table_path(config, "customers_bronze")
customers_silver_path = table_path(config, "customers_silver")
products_bronze_path = table_path(config, "products_bronze")
products_silver_path = table_path(config, "products_silver")

customers = spark.read.format("delta").load(customers_bronze_path)
customers_rows_read = customers.count()

customers_silver = (
    customers.withColumn("customer_name", trim(col("customer_name")))
    .withColumn("email", lower(trim(col("email"))))
    .withColumn("country", trim(col("country")))
    .withColumn("signup_date", to_date(col("signup_date")))
)

customers_write_mode = merge_dimension_scd2(
    spark,
    customers_silver_path,
    customers_silver,
    key_column="customer_id",
    business_columns=[
        "customer_id",
        "customer_name",
        "email",
        "country",
        "signup_date",
    ],
)
customers_rows_written = spark.read.format("delta").load(customers_silver_path).count()


products = spark.read.format("delta").load(products_bronze_path)
products_rows_read = products.count()

products_silver = (
    products.withColumn("product_name", trim(col("product_name")))
    .withColumn("category", trim(col("category")))
    .withColumn("unit_cost", col("unit_cost").cast("double"))
)

products_write_mode = merge_dimension_scd2(
    spark,
    products_silver_path,
    products_silver,
    key_column="product_id",
    business_columns=[
        "product_id",
        "product_name",
        "category",
        "unit_cost",
    ],
)
products_rows_written = spark.read.format("delta").load(products_silver_path).count()

print("Silver customers:")
mask_customer_columns(spark.read.format("delta").load(customers_silver_path)).show()

print("Silver products:")
spark.read.format("delta").load(products_silver_path).show()

write_step_metric(
    "silver_customers_products",
    rows_read=customers_rows_read + products_rows_read,
    rows_written=customers_rows_written + products_rows_written,
    rows_quarantined=0,
    duration_seconds=round(time.time() - start_time, 2),
    input_path=[customers_bronze_path, products_bronze_path],
    output_path=[customers_silver_path, products_silver_path],
    details={
        "customers_write_mode": customers_write_mode,
        "products_write_mode": products_write_mode,
        "dimension_strategy": "scd_type_2",
        "current_record_column": "is_current",
    },
)

spark.stop()
