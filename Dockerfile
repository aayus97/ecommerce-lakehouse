FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    SPARK_LOCAL_IP=127.0.0.1 \
    SPARK_IVY_DIR=/opt/spark-ivy

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        bash \
        curl \
        libnss-wrapper \
        make \
        openjdk-21-jre-headless \
        procps \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY docker/entrypoint.sh /usr/local/bin/lakehouse-entrypoint
RUN chmod +x /usr/local/bin/lakehouse-entrypoint

COPY requirements.txt .
RUN python -m pip install --upgrade pip \
    && python -m pip install --no-cache-dir -r requirements.txt

RUN mkdir -p "${SPARK_IVY_DIR}" \
    && chmod -R 777 "${SPARK_IVY_DIR}" \
    && python -c "from delta import configure_spark_with_delta_pip; from pyspark.sql import SparkSession; spark = configure_spark_with_delta_pip(SparkSession.builder.appName('DeltaDependencyWarmup').master('local[1]').config('spark.jars.ivy', '${SPARK_IVY_DIR}').config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension').config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')).getOrCreate(); spark.range(1).count(); spark.stop()" \
    && chmod -R 777 "${SPARK_IVY_DIR}"

COPY . .

ENTRYPOINT ["lakehouse-entrypoint"]
CMD ["python", "run_pipeline.py"]
