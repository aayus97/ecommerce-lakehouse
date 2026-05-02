from pyspark.sql.functions import sum, countDistinct, count
import time

from src.backfill import (
    affected_order_dates,
    backfill_metric_details,
    filter_by_order_date,
)
from src.config import load_app_config, table_path
from src.delta_utils import write_date_partitions_delta
from src.metrics import write_step_metric
from src.spark_session import get_spark

config = load_app_config()

spark = get_spark("GoldSalesSummary")
start_time = time.time()
orders_silver_path = table_path(config, "orders_silver")
daily_sales_summary_path = table_path(config, "daily_sales_summary")

silver = filter_by_order_date(spark.read.format("delta").load(orders_silver_path))
rows_read = silver.count()
affected_dates = affected_order_dates(silver)

gold = silver.groupBy("order_date").agg(
    count("order_id").alias("total_orders"),
    countDistinct("customer_id").alias("unique_customers"),
    sum("total_amount").alias("daily_revenue"),
)

write_mode = write_date_partitions_delta(
    spark,
    gold,
    daily_sales_summary_path,
    affected_dates,
)
rows_written = spark.read.format("delta").load(daily_sales_summary_path).count()

print("Gold daily sales summary created")
spark.read.format("delta").load(daily_sales_summary_path).show()

write_step_metric(
    "gold_daily_sales",
    rows_read=rows_read,
    rows_written=rows_written,
    rows_quarantined=0,
    duration_seconds=round(time.time() - start_time, 2),
    input_path=orders_silver_path,
    output_path=daily_sales_summary_path,
    details={
        "write_mode": write_mode,
        "partition_columns": ["order_date"],
        "affected_dates": affected_dates,
        **backfill_metric_details(),
    },
)

spark.stop()
