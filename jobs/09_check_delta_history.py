from delta.tables import DeltaTable
from src.spark_session import get_spark

spark = get_spark("CheckDeltaHistory")

delta_table = DeltaTable.forPath(spark, "data/bronze/orders")

delta_table.history().show(truncate=False)

spark.stop()