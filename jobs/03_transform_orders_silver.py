from pyspark.sql.functions import col, round as spark_round, to_date
import time

from src.config import load_app_config, table_path
from src.delta_utils import write_partitioned_delta
from src.metrics import write_step_metric
from src.spark_session import get_spark

config = load_app_config()

spark = get_spark("TransformOrdersSilver")
start_time = time.time()
orders_validated_path = table_path(config, "orders_validated")
orders_silver_path = table_path(config, "orders_silver")

bronze = spark.read.format("delta").load(orders_validated_path)
rows_read = bronze.count()

silver = (
    bronze.withColumn("order_date", to_date(col("order_date")))
    .withColumn("quantity", col("quantity").cast("int"))
    .withColumn("unit_price", col("unit_price").cast("double"))
    .withColumn("total_amount", spark_round(col("quantity") * col("unit_price"), 2))
    .filter(col("status") == "completed")
)

write_partitioned_delta(silver, orders_silver_path, partition_columns=["order_date"])
rows_written = spark.read.format("delta").load(orders_silver_path).count()

print("Silver orders table created")
spark.read.format("delta").load(orders_silver_path).show()

write_step_metric(
    "silver_orders",
    rows_read=rows_read,
    rows_written=rows_written,
    rows_quarantined=0,
    duration_seconds=round(time.time() - start_time, 2),
    input_path=orders_validated_path,
    output_path=orders_silver_path,
    details={"partition_columns": ["order_date"]},
)

spark.stop()
