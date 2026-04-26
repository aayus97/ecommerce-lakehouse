from src.spark_session import get_spark

spark = get_spark("ViewQuarantineOrders")

df = spark.read.format("delta").load("data/quarantine/orders")

print("Quarantined orders:")
df.show(truncate=False)

spark.stop()