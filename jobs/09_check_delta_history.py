from src.config import load_app_config, table_path
from src.delta_utils import assert_delta_history_has_operations
from src.spark_session import get_spark

config = load_app_config()
spark = get_spark("CheckDeltaHistory")

orders_bronze_path = table_path(config, "orders_bronze")
history = assert_delta_history_has_operations(
    spark,
    orders_bronze_path,
    expected_operations={"WRITE", "MERGE"},
)

print(f"Delta history audit passed for {orders_bronze_path}")
history.show(truncate=False)

spark.stop()
