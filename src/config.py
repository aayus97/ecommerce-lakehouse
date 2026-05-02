import os
from pathlib import Path

import yaml

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None


if load_dotenv:
    load_dotenv()


def get_environment():
    return os.getenv("APP_ENV", "dev")


def load_app_config(env=None):
    selected_env = env or get_environment()
    storage_mode_override = os.getenv("STORAGE_MODE")

    if env is None and selected_env == "dev" and storage_mode_override == "minio":
        selected_env = "minio"

    config_path = Path("configs") / f"{selected_env}.yaml"

    with config_path.open("r") as f:
        return yaml.safe_load(f)


def storage_mode(config):
    return os.getenv("STORAGE_MODE") or config.get("storage", {}).get("mode", "local")


def storage_config(config):
    return config.get("storage", {})


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
