import os

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

from src.config import load_app_config, storage_config, storage_mode

HADOOP_AWS_PACKAGE = "org.apache.hadoop:hadoop-aws:3.4.1"


def get_spark(app_name: str):
    app_config = load_app_config()
    selected_storage_mode = storage_mode(app_config)
    ivy_dir = os.getenv("SPARK_IVY_DIR", "/tmp/spark-ivy")
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.jars.ivy", ivy_dir)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )

    extra_packages = []
    if selected_storage_mode in {"minio", "s3"}:
        extra_packages.append(HADOOP_AWS_PACKAGE)
        builder = configure_s3a(builder, app_config)

    return configure_spark_with_delta_pip(
        builder,
        extra_packages=extra_packages,
    ).getOrCreate()


def configure_s3a(builder, app_config):
    config = storage_config(app_config)
    endpoint = os.getenv("MINIO_ENDPOINT") or config.get("endpoint")
    access_key = (
        os.getenv("AWS_ACCESS_KEY_ID")
        or os.getenv("MINIO_ROOT_USER")
        or config.get("access_key")
    )
    secret_key = (
        os.getenv("AWS_SECRET_ACCESS_KEY")
        or os.getenv("MINIO_ROOT_PASSWORD")
        or config.get("secret_key")
    )

    builder = (
        builder.config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config(
            "spark.hadoop.fs.s3a.path.style.access",
            str(config.get("path_style_access", True)).lower(),
        )
        .config(
            "spark.hadoop.fs.s3a.connection.ssl.enabled",
            str(config.get("ssl_enabled", False)).lower(),
        )
    )

    if endpoint:
        builder = builder.config("spark.hadoop.fs.s3a.endpoint", endpoint)
    if access_key:
        builder = builder.config("spark.hadoop.fs.s3a.access.key", access_key)
    if secret_key:
        builder = builder.config("spark.hadoop.fs.s3a.secret.key", secret_key)

    return builder
