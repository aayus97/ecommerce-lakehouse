from datetime import datetime, timezone
from pathlib import Path
import time

from pyspark.sql.functions import (
    col,
    count,
    max as spark_max,
    round as spark_round,
    sum,
)

from src.config import load_app_config, table_path
from src.logger import get_logger
from src.metrics import write_metric, write_step_metric
from src.spark_session import get_spark

config = load_app_config()
spark = get_spark("CollectGoldMetrics")
logger = get_logger("gold_metrics")
start_time = time.time()

daily_sales_path = table_path(config, "daily_sales_summary")
revenue_by_category_country_path = table_path(config, "revenue_by_category_country")
orders_silver_path = table_path(config, "orders_silver")

daily_sales = spark.read.format("delta").load(daily_sales_path)
revenue_by_category_country = spark.read.format("delta").load(
    revenue_by_category_country_path
)
orders_silver = spark.read.format("delta").load(orders_silver_path)

summary = daily_sales.agg(
    count("*").alias("total_days"),
    sum("total_orders").alias("total_orders"),
    sum("daily_revenue").alias("total_revenue"),
).collect()[0]

total_orders = int(summary["total_orders"] or 0)
total_revenue = float(summary["total_revenue"] or 0)
average_order_value = round(total_revenue / total_orders, 2) if total_orders else 0.0

top_countries = [
    {
        "country": row["country"],
        "total_revenue": float(row["total_revenue"] or 0),
        "total_orders": int(row["total_orders"] or 0),
    }
    for row in (
        revenue_by_category_country.groupBy("country")
        .agg(
            spark_round(sum("revenue"), 2).alias("total_revenue"),
            sum("total_orders").alias("total_orders"),
        )
        .orderBy(col("total_revenue").desc())
        .limit(5)
        .collect()
    )
]

top_categories = [
    {
        "category": row["category"],
        "total_revenue": float(row["total_revenue"] or 0),
        "total_orders": int(row["total_orders"] or 0),
    }
    for row in (
        revenue_by_category_country.groupBy("category")
        .agg(
            spark_round(sum("revenue"), 2).alias("total_revenue"),
            sum("total_orders").alias("total_orders"),
        )
        .orderBy(col("total_revenue").desc())
        .limit(5)
        .collect()
    )
]

latest_order_date = orders_silver.agg(
    spark_max("order_date").alias("latest_order_date")
).collect()[0]["latest_order_date"]


def latest_local_mtime(path):
    local_path = Path(path)
    if not local_path.exists():
        return None

    latest_timestamp = max(
        (item.stat().st_mtime for item in local_path.rglob("*") if item.is_file()),
        default=local_path.stat().st_mtime,
    )
    return datetime.fromtimestamp(latest_timestamp, timezone.utc).isoformat()


metrics = {
    "total_days": int(summary["total_days"] or 0),
    "total_orders": total_orders,
    "total_revenue": total_revenue,
    "average_order_value": average_order_value,
    "top_countries": top_countries,
    "top_categories": top_categories,
    "latest_order_date": latest_order_date.isoformat() if latest_order_date else None,
    "gold_table_last_updated_timestamp": latest_local_mtime(daily_sales_path),
    "daily_sales_summary_path": daily_sales_path,
    "revenue_by_category_country_path": revenue_by_category_country_path,
}

logger.info(f"Gold metrics: {metrics}")

write_metric("gold_sales_metrics", metrics)
write_metric(
    "freshness_metrics",
    {
        "latest_order_date": metrics["latest_order_date"],
        "gold_table_last_updated_timestamp": metrics[
            "gold_table_last_updated_timestamp"
        ],
        "daily_sales_summary_path": daily_sales_path,
        "revenue_by_category_country_path": revenue_by_category_country_path,
    },
)

write_step_metric(
    "collect_gold_metrics",
    rows_read=daily_sales.count()
    + revenue_by_category_country.count()
    + orders_silver.count(),
    rows_written=2,
    rows_quarantined=0,
    duration_seconds=round(time.time() - start_time, 2),
    input_path=[
        daily_sales_path,
        revenue_by_category_country_path,
        orders_silver_path,
    ],
    output_path="metrics/*.jsonl",
)

spark.stop()
