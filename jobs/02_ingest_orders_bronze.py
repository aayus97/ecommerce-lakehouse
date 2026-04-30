from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

from src.config import load_app_config, table_path

config = load_app_config()


builder = (
    SparkSession.builder.appName("IngestOrdersBronze")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

df = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv(table_path(config, "orders_raw"))
)

df.write.format("delta").mode("overwrite").save(table_path(config, "orders_bronze"))

print("Bronze orders table created")
spark.read.format("delta").load(table_path(config, "orders_bronze")).show()

spark.stop()
