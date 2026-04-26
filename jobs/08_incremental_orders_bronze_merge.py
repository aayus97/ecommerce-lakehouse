from delta.tables import DeltaTable
from src.spark_session import get_spark

spark = get_spark("IncrementalOrdersBronzeMerge")

source_path = "data/raw/orders_batch_2.csv"
target_path = "data/bronze/orders"

updates = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(source_path)
)

delta_table = DeltaTable.forPath(spark, target_path)

(
    delta_table.alias("target")
    .merge(
        updates.alias("source"),
        "target.order_id = source.order_id"
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

print("Bronze orders merged successfully")

spark.read.format("delta").load(target_path).orderBy("order_id").show()

spark.stop()