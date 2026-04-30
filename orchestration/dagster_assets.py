import os
import subprocess
import sys

from dagster import AssetExecutionContext, RetryPolicy, asset

PIPELINE_NAME = "ecommerce_lakehouse"
ASSET_GROUP = "lakehouse_pipeline"


def _run_job_module(context: AssetExecutionContext, module_name: str) -> None:
    env = os.environ.copy()
    env["PIPELINE_NAME"] = PIPELINE_NAME
    env["PIPELINE_RUN_ID"] = context.run_id

    context.log.info("Running module %s", module_name)
    result = subprocess.run(
        [sys.executable, "-m", module_name],
        env=env,
        text=True,
    )

    if result.returncode != 0:
        raise RuntimeError(
            f"Module {module_name} failed with return code {result.returncode}"
        )


@asset(
    group_name=ASSET_GROUP,
    retry_policy=RetryPolicy(max_retries=2, delay=30),
    description="Incrementally merge new order records into the bronze Delta table.",
)
def bronze_merge(context: AssetExecutionContext) -> None:
    _run_job_module(context, "jobs.08_incremental_orders_bronze_merge")


@asset(
    deps=[bronze_merge],
    group_name=ASSET_GROUP,
    retry_policy=RetryPolicy(max_retries=1, delay=30),
    description="Validate bronze orders and quarantine invalid rows.",
)
def validate_orders(context: AssetExecutionContext) -> None:
    _run_job_module(context, "jobs.12_validate_and_quarantine_orders")


@asset(
    deps=[validate_orders],
    group_name=ASSET_GROUP,
    retry_policy=RetryPolicy(max_retries=1, delay=30),
    description="Transform validated bronze orders into the silver orders table.",
)
def silver_orders(context: AssetExecutionContext) -> None:
    _run_job_module(context, "jobs.03_transform_orders_silver")


@asset(
    deps=[silver_orders],
    group_name=ASSET_GROUP,
    retry_policy=RetryPolicy(max_retries=1, delay=30),
    description="Build the gold daily sales summary table.",
)
def gold_daily_sales(context: AssetExecutionContext) -> None:
    _run_job_module(context, "jobs.04_create_gold_sales_summary")


@asset(
    deps=[silver_orders],
    group_name=ASSET_GROUP,
    retry_policy=RetryPolicy(max_retries=1, delay=30),
    description="Build gold revenue aggregates by category and country.",
)
def gold_revenue(context: AssetExecutionContext) -> None:
    _run_job_module(context, "jobs.07_create_gold_revenue_by_category_country")


@asset(
    deps=[gold_daily_sales, gold_revenue],
    group_name=ASSET_GROUP,
    retry_policy=RetryPolicy(max_retries=1, delay=30),
    description="Collect gold-layer business metrics for monitoring.",
)
def collect_gold_metrics(context: AssetExecutionContext) -> None:
    _run_job_module(context, "jobs.14_collect_gold_metrics")


lakehouse_assets = [
    bronze_merge,
    validate_orders,
    silver_orders,
    gold_daily_sales,
    gold_revenue,
    collect_gold_metrics,
]
