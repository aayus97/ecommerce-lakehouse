from src.config import load_app_config

REQUIRED_TABLES = [
    "orders_raw",
    "orders_batch_2",
    "orders_bad_batch",
    "customers_raw",
    "products_raw",
    "orders_bronze",
    "orders_validated",
    "orders_quarantine",
    "orders_silver",
    "customers_bronze",
    "products_bronze",
    "customers_silver",
    "products_silver",
    "daily_sales_summary",
    "revenue_by_category_country",
]


REQUIRED_PATHS = [
    "raw",
    "bronze",
    "silver",
    "gold",
    "quarantine",
    "metrics",
]


def test_all_env_configs_have_required_tables():
    for env in ["dev", "test", "prod"]:
        config = load_app_config(env)

        for table in REQUIRED_TABLES:
            assert table in config["tables"]


def test_all_env_configs_have_required_paths():
    for env in ["dev", "test", "prod"]:
        config = load_app_config(env)

        for path_name in REQUIRED_PATHS:
            assert path_name in config["paths"]
