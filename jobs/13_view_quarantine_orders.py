from src.config import load_app_config, table_path
from src.spark_session import get_spark

config = load_app_config()
spark = get_spark("ViewQuarantineOrders")

df = spark.read.format("delta").load(table_path(config, "orders_quarantine"))

print("Quarantined orders:")
df.show(truncate=False)

spark.stop()
