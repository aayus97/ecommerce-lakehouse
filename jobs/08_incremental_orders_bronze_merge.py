import time

from src.backfill import backfill_metric_details, filter_by_order_date
from src.config import load_app_config, table_path
from src.delta_utils import merge_orders_by_id
from src.metrics import write_step_metric
from src.spark_session import get_spark

config = load_app_config()
spark = get_spark("IncrementalOrdersBronzeMerge")
start_time = time.time()

source_path = table_path(config, "orders_batch_2")
target_path = table_path(config, "orders_bronze")

updates = filter_by_order_date(
    spark.read.option("header", True).option("inferSchema", True).csv(source_path)
)
rows_read = updates.count()

merge_orders_by_id(spark, target_path, updates)

print("Bronze orders merged successfully")

merged_orders = spark.read.format("delta").load(target_path)
rows_written = merged_orders.count()
merged_orders.orderBy("order_id").show()

write_step_metric(
    "bronze_merge",
    rows_read=rows_read,
    rows_written=rows_written,
    rows_quarantined=0,
    duration_seconds=round(time.time() - start_time, 2),
    input_path=source_path,
    output_path=target_path,
    details={
        "merge_key": "order_id",
        "deduplication": "latest_source_update_ts",
        **backfill_metric_details(),
    },
)

spark.stop()
