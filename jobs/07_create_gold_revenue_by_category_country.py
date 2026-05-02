from pyspark.sql.functions import count, countDistinct, round as spark_round, sum
import time

from src.config import load_app_config, table_path
from src.delta_utils import write_date_partitions_delta
from src.metrics import write_step_metric
from src.spark_session import get_spark

config = load_app_config()
spark = get_spark("GoldRevenueByCategoryCountry")
start_time = time.time()
orders_silver_path = table_path(config, "orders_silver")
customers_silver_path = table_path(config, "customers_silver")
products_silver_path = table_path(config, "products_silver")
revenue_by_category_country_path = table_path(config, "revenue_by_category_country")

orders = spark.read.format("delta").load(orders_silver_path)
customers = spark.read.format("delta").load(customers_silver_path)
products = spark.read.format("delta").load(products_silver_path)
rows_read = orders.count() + customers.count() + products.count()
affected_dates = [
    row["order_date"].isoformat()
    for row in orders.select("order_date").distinct().collect()
    if row["order_date"] is not None
]

joined = orders.join(customers, on="customer_id", how="left").join(
    products, on="product_id", how="left"
)

gold = (
    joined.groupBy("order_date", "country", "category")
    .agg(
        count("order_id").alias("total_orders"),
        countDistinct("customer_id").alias("unique_customers"),
        spark_round(sum("total_amount"), 2).alias("revenue"),
    )
    .orderBy("order_date", "country", "category")
)

write_mode = write_date_partitions_delta(
    spark,
    gold,
    revenue_by_category_country_path,
    affected_dates,
)
rows_written = spark.read.format("delta").load(revenue_by_category_country_path).count()

gold.show()

write_step_metric(
    "gold_revenue",
    rows_read=rows_read,
    rows_written=rows_written,
    rows_quarantined=0,
    duration_seconds=round(time.time() - start_time, 2),
    input_path=[orders_silver_path, customers_silver_path, products_silver_path],
    output_path=revenue_by_category_country_path,
    details={
        "write_mode": write_mode,
        "partition_columns": ["order_date"],
        "affected_dates": affected_dates,
    },
)

spark.stop()
