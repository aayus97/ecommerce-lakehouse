import time

from src.config import load_app_config, table_path
from src.metrics import write_step_metric
from src.spark_session import get_spark

config = load_app_config()
spark = get_spark("IngestCustomersProductsBronze")
start_time = time.time()

tables = ["customers", "products"]
total_rows_read = 0
total_rows_written = 0
input_paths = []
output_paths = []

for table in tables:
    raw_path = table_path(config, f"{table}_raw")
    bronze_path = table_path(config, f"{table}_bronze")
    input_paths.append(raw_path)
    output_paths.append(bronze_path)

    df = spark.read.option("header", True).option("inferSchema", True).csv(raw_path)
    rows_read = df.count()
    total_rows_read += rows_read

    df.write.format("delta").mode("overwrite").save(bronze_path)
    rows_written = spark.read.format("delta").load(bronze_path).count()
    total_rows_written += rows_written

    print(f"Bronze table created: {table}")
    spark.read.format("delta").load(bronze_path).show()

write_step_metric(
    "ingest_customers_products_bronze",
    rows_read=total_rows_read,
    rows_written=total_rows_written,
    rows_quarantined=0,
    duration_seconds=round(time.time() - start_time, 2),
    input_path=input_paths,
    output_path=output_paths,
)

spark.stop()
