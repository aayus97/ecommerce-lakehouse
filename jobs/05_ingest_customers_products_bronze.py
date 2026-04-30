from src.config import load_app_config, table_path
from src.spark_session import get_spark

config = load_app_config()
spark = get_spark("IngestCustomersProductsBronze")

tables = ["customers", "products"]

for table in tables:
    raw_path = table_path(config, f"{table}_raw")
    bronze_path = table_path(config, f"{table}_bronze")

    df = spark.read.option("header", True).option("inferSchema", True).csv(raw_path)

    df.write.format("delta").mode("overwrite").save(bronze_path)

    print(f"Bronze table created: {table}")
    spark.read.format("delta").load(bronze_path).show()

spark.stop()
