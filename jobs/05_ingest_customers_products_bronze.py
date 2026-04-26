from src.spark_session import get_spark

spark = get_spark("IngestCustomersProductsBronze")

tables = ["customers", "products"]

for table in tables:
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(f"data/raw/{table}.csv")
    )

    df.write.format("delta").mode("overwrite").save(f"data/bronze/{table}")

    print(f"Bronze table created: {table}")
    spark.read.format("delta").load(f"data/bronze/{table}").show()

spark.stop()