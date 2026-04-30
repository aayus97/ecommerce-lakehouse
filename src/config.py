import os
from pathlib import Path

import yaml


def get_environment():
    return os.getenv("APP_ENV", "dev")


def load_app_config(env=None):
    selected_env = env or get_environment()
    config_path = Path("configs") / f"{selected_env}.yaml"

    with config_path.open("r") as f:
        return yaml.safe_load(f)


def table_path(config, name):
    tables = config.get("tables", {})

    if name not in tables:
        raise KeyError(f"Missing table path in config: tables.{name}")

    return tables[name]


def path_value(config, name):
    paths = config.get("paths", {})

    if name not in paths:
        raise KeyError(f"Missing path in config: paths.{name}")

    return paths[name]
