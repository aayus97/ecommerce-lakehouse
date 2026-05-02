import os
from datetime import date

from pyspark.sql.functions import col, lit, to_date

BACKFILL_START_DATE_ENV = "BACKFILL_START_DATE"
BACKFILL_END_DATE_ENV = "BACKFILL_END_DATE"


def parse_date(value: str | None) -> date | None:
    if not value:
        return None
    return date.fromisoformat(value)


def get_backfill_window() -> tuple[str | None, str | None]:
    return os.getenv(BACKFILL_START_DATE_ENV), os.getenv(BACKFILL_END_DATE_ENV)


def is_backfill_run() -> bool:
    start_date, end_date = get_backfill_window()
    return bool(start_date or end_date)


def filter_by_order_date(df, column_name: str = "order_date"):
    start_date, end_date = get_backfill_window()
    filtered = df.withColumn(column_name, to_date(col(column_name)))

    if start_date:
        filtered = filtered.filter(col(column_name) >= to_date(lit(start_date)))
    if end_date:
        filtered = filtered.filter(col(column_name) <= to_date(lit(end_date)))

    return filtered


def affected_order_dates(df, column_name: str = "order_date") -> list[str]:
    return sorted(
        row[column_name].isoformat()
        for row in df.select(column_name).distinct().collect()
        if row[column_name] is not None
    )


def backfill_metric_details() -> dict:
    start_date, end_date = get_backfill_window()
    return {
        "backfill_start_date": start_date,
        "backfill_end_date": end_date,
        "is_backfill": is_backfill_run(),
    }
