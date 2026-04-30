import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
TEST_LAKEHOUSE_ROOT = Path("/tmp/ecommerce-lakehouse-test")
PIPELINE_MODULES = [
    "jobs.02_ingest_orders_bronze",
    "jobs.12_validate_and_quarantine_orders",
    "jobs.03_transform_orders_silver",
    "jobs.05_ingest_customers_products_bronze",
    "jobs.06_transform_customers_products_silver",
    "jobs.04_create_gold_sales_summary",
    "jobs.07_create_gold_revenue_by_category_country",
]


@pytest.fixture(scope="session")
def mini_pipeline_outputs():
    if TEST_LAKEHOUSE_ROOT.exists():
        shutil.rmtree(TEST_LAKEHOUSE_ROOT)

    env = os.environ.copy()
    env["APP_ENV"] = "test"
    env["PYTHONPATH"] = str(REPO_ROOT)
    env["PIPELINE_NAME"] = "pytest-mini-pipeline"
    env["PIPELINE_RUN_ID"] = "pytest-regression"

    for module in PIPELINE_MODULES:
        subprocess.run(
            [sys.executable, "-m", module],
            cwd=REPO_ROOT,
            env=env,
            check=True,
            text=True,
            capture_output=True,
        )

    return {
        "root": TEST_LAKEHOUSE_ROOT,
        "bronze_orders": TEST_LAKEHOUSE_ROOT / "bronze" / "orders",
        "orders_validated": TEST_LAKEHOUSE_ROOT / "bronze" / "orders_validated",
        "orders_silver": TEST_LAKEHOUSE_ROOT / "silver" / "orders",
        "customers_silver": TEST_LAKEHOUSE_ROOT / "silver" / "customers",
        "products_silver": TEST_LAKEHOUSE_ROOT / "silver" / "products",
        "daily_sales_summary": TEST_LAKEHOUSE_ROOT / "gold" / "daily_sales_summary",
        "revenue_by_category_country": (
            TEST_LAKEHOUSE_ROOT / "gold" / "revenue_by_category_country"
        ),
    }
