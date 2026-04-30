from delta.tables import DeltaTable

from src.config import load_app_config, table_path
from src.spark_session import get_spark

config = load_app_config()
spark = get_spark("CheckDeltaHistory")

delta_table = DeltaTable.forPath(spark, table_path(config, "orders_bronze"))

delta_table.history().show(truncate=False)

spark.stop()
