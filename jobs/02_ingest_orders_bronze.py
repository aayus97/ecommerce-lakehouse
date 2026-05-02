import time

from src.config import load_app_config, table_path
from src.delta_utils import merge_orders_by_id
from src.metrics import write_step_metric
from src.spark_session import get_spark

config = load_app_config()

spark = get_spark("IngestOrdersBronze")
start_time = time.time()
orders_raw_path = table_path(config, "orders_raw")
orders_bronze_path = table_path(config, "orders_bronze")

df = spark.read.option("header", True).option("inferSchema", True).csv(orders_raw_path)
rows_read = df.count()

write_mode = merge_orders_by_id(spark, orders_bronze_path, df)
rows_written = spark.read.format("delta").load(orders_bronze_path).count()

print(f"Bronze orders table {write_mode}")
spark.read.format("delta").load(orders_bronze_path).show()

write_step_metric(
    "ingest_orders_bronze",
    rows_read=rows_read,
    rows_written=rows_written,
    rows_quarantined=0,
    duration_seconds=round(time.time() - start_time, 2),
    input_path=orders_raw_path,
    output_path=orders_bronze_path,
    details={"write_mode": write_mode, "merge_key": "order_id"},
)

spark.stop()
