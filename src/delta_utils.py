from delta.tables import DeltaTable
from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (
    col,
    concat_ws,
    current_date,
    current_timestamp,
    lit,
    row_number,
    sha2,
    to_date,
    to_timestamp,
)

ORDER_BUSINESS_COLUMNS = [
    "order_id",
    "customer_id",
    "product_id",
    "order_date",
    "quantity",
    "unit_price",
    "status",
]
ORDER_METADATA_COLUMNS = [
    "source_update_ts",
    "ingestion_ts",
    "ingestion_date",
    "record_hash",
]
ORDER_PARTITION_COLUMNS = ["order_date"]


def normalize_orders_for_delta(
    orders: DataFrame,
    default_source_update_ts=None,
) -> DataFrame:
    normalized = orders.withColumn("order_date", to_date(col("order_date")))

    update_timestamp_candidates = [
        column
        for column in ("update_timestamp", "updated_at", "source_update_ts")
        if column in normalized.columns
    ]
    if update_timestamp_candidates:
        source_update_ts = to_timestamp(col(update_timestamp_candidates[0]))
    elif default_source_update_ts:
        source_update_ts = to_timestamp(lit(default_source_update_ts))
    else:
        source_update_ts = current_timestamp()

    normalized = (
        normalized.withColumn("source_update_ts", source_update_ts)
        .withColumn("ingestion_ts", current_timestamp())
        .withColumn("ingestion_date", current_date())
    )

    hash_columns = [
        col(column).cast("string")
        for column in ORDER_BUSINESS_COLUMNS
        if column in normalized.columns
    ]
    normalized = normalized.withColumn(
        "record_hash",
        sha2(concat_ws("||", *hash_columns), 256),
    )
    selected_columns = [
        column
        for column in [*ORDER_BUSINESS_COLUMNS, *ORDER_METADATA_COLUMNS]
        if column in normalized.columns
    ]
    return normalized.select(*selected_columns)


def latest_orders_by_id(orders: DataFrame) -> DataFrame:
    window = Window.partitionBy("order_id").orderBy(
        col("source_update_ts").desc_nulls_last(),
        col("ingestion_ts").desc_nulls_last(),
        col("record_hash").desc_nulls_last(),
    )

    return (
        orders.withColumn("_order_rank", row_number().over(window))
        .filter(col("_order_rank") == 1)
        .drop("_order_rank")
    )


def prepare_orders_for_upsert(orders: DataFrame) -> DataFrame:
    return latest_orders_by_id(normalize_orders_for_delta(orders))


def merge_orders_by_id(spark, target_path: str, updates: DataFrame) -> str:
    prepared_updates = prepare_orders_for_upsert(updates)

    if not DeltaTable.isDeltaTable(spark, target_path):
        (
            prepared_updates.write.format("delta")
            .mode("overwrite")
            .partitionBy(*ORDER_PARTITION_COLUMNS)
            .save(target_path)
        )
        return "created"

    target = spark.read.format("delta").load(target_path)
    if set(ORDER_METADATA_COLUMNS) - set(target.columns):
        existing_orders = normalize_orders_for_delta(
            target,
            default_source_update_ts="1970-01-01 00:00:00",
        )
        migrated_orders = latest_orders_by_id(
            existing_orders.unionByName(prepared_updates, allowMissingColumns=True)
        )
        migrated_orders.cache()
        migrated_orders.count()
        write_partitioned_delta(migrated_orders, target_path)
        migrated_orders.unpersist()
        return "migrated"

    delta_table = DeltaTable.forPath(spark, target_path)
    update_assignments = {
        column: f"source.{column}" for column in prepared_updates.columns
    }

    (
        delta_table.alias("target")
        .merge(prepared_updates.alias("source"), "target.order_id = source.order_id")
        .whenMatchedUpdate(
            condition=(
                "source.source_update_ts >= target.source_update_ts "
                "AND source.record_hash <> target.record_hash"
            ),
            set=update_assignments,
        )
        .whenNotMatchedInsertAll()
        .execute()
    )
    return "merged"


def write_partitioned_delta(df: DataFrame, path: str, partition_columns=None):
    partition_columns = partition_columns or ORDER_PARTITION_COLUMNS
    writer = (
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    )

    if partition_columns:
        writer = writer.partitionBy(*partition_columns)

    writer.save(path)


def write_date_partitions_delta(
    spark,
    df: DataFrame,
    path: str,
    affected_dates: list[str],
    partition_column: str = "order_date",
) -> str:
    if not affected_dates:
        return "skipped"

    if not DeltaTable.isDeltaTable(spark, path):
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .partitionBy(partition_column)
            .save(path)
        )
        return "created"

    delta_table = DeltaTable.forPath(spark, path)
    detail = delta_table.detail().select("partitionColumns").collect()[0]
    partition_columns = detail["partitionColumns"] or []

    if partition_columns != [partition_column]:
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .partitionBy(partition_column)
            .save(path)
        )
        return "repartitioned"

    quoted_dates = ", ".join(f"'{date_value}'" for date_value in sorted(affected_dates))
    replace_where = f"{partition_column} IN ({quoted_dates})"
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", replace_where)
        .save(path)
    )
    return "replaced_partitions"


def assert_delta_history_has_operations(
    spark,
    table_path: str,
    expected_operations: set[str] | None = None,
):
    expected_operations = expected_operations or {"WRITE", "MERGE"}
    history = DeltaTable.forPath(spark, table_path).history()
    operations = {row["operation"] for row in history.select("operation").collect()}
    missing_operations = expected_operations - operations

    if missing_operations:
        raise AssertionError(
            "Delta history missing expected operations for "
            f"{table_path}: {sorted(missing_operations)}. "
            f"Observed operations: {sorted(operations)}"
        )

    return history
