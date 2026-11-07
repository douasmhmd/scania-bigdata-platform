"""Test de fumée : vérifie que le cluster Spark écrit et relit dans HDFS."""
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("smoke-test-spark-hdfs").getOrCreate()

path = "hdfs://namenode:8020/lakehouse/_smoke/parquet"

# Écriture distribuée : 4 partitions réparties sur les 2 workers
df = spark.range(0, 1_000_000).withColumnRenamed("id", "value")
df.repartition(4).write.mode("overwrite").parquet(path)

# Relecture depuis HDFS
count = spark.read.parquet(path).count()
print(f"SMOKE TEST OK - lignes relues depuis HDFS : {count}")

spark.stop()