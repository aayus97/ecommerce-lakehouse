from pyspark.sql.functions import sum, count

from src.config import load_app_config, table_path
from src.logger import get_logger
from src.metrics import write_metric
from src.spark_session import get_spark

config = load_app_config()
spark = get_spark("CollectGoldMetrics")
logger = get_logger("gold_metrics")

daily_sales = spark.read.format("delta").load(table_path(config, "daily_sales_summary"))

summary = daily_sales.agg(
    count("*").alias("total_days"),
    sum("total_orders").alias("total_orders"),
    sum("daily_revenue").alias("total_revenue"),
).collect()[0]

metrics = {
    "total_days": summary["total_days"],
    "total_orders": summary["total_orders"],
    "total_revenue": float(summary["total_revenue"]),
}

logger.info(f"Gold metrics: {metrics}")

write_metric("gold_sales_metrics", metrics)

spark.stop()
