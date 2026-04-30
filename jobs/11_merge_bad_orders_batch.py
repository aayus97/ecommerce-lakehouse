from delta.tables import DeltaTable

from src.config import load_app_config, table_path
from src.spark_session import get_spark

config = load_app_config()
spark = get_spark("MergeBadOrdersBatch")

source_path = table_path(config, "orders_bad_batch")
target_path = table_path(config, "orders_bronze")

updates = spark.read.option("header", True).option("inferSchema", True).csv(source_path)

delta_table = DeltaTable.forPath(spark, target_path)

(
    delta_table.alias("target")
    .merge(updates.alias("source"), "target.order_id = source.order_id")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

print("Bad batch merged into Bronze")

spark.read.format("delta").load(target_path).orderBy("order_id").show()

spark.stop()
