"""
Job Structured Streaming - couche Bronze.
Lit les evenements du topic Kafka scania.readouts et les archive dans HDFS
en Parquet, avec les metadonnees Kafka pour la tracabilite.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, IntegerType, LongType,
                               DoubleType, StringType, MapType)

KAFKA_BOOTSTRAP = "kafka:29092"
TOPIC = "scania.readouts"
BRONZE_PATH = "hdfs://namenode:8020/lakehouse/bronze/readouts"
CHECKPOINT = "hdfs://namenode:8020/checkpoints/bronze_readouts"

# Contrat d'evenement v1 (meme structure que ingestion/producer.py)
EVENT_SCHEMA = StructType([
    StructField("schema_version", IntegerType()),
    StructField("event_id", StringType()),
    StructField("vehicle_id", LongType()),
    StructField("time_step", DoubleType()),
    StructField("produced_at", StringType()),
    StructField("readings", MapType(StringType(), DoubleType())),
])

spark = SparkSession.builder.appName("bronze-ingest-readouts").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Source : le topic Kafka
raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
       .option("subscribe", TOPIC)
       .option("startingOffsets", "earliest")   # 1er lancement : tout l'historique du topic
       .option("maxOffsetsPerTrigger", 50000)    # taille maximale d'un micro-batch
       .load())

# Parsing du JSON + metadonnees Kafka (tracabilite)
bronze = (raw
    .select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_timestamp"))
    .select("e.*", "kafka_partition", "kafka_offset", "kafka_timestamp")
    .withColumn("produced_at", F.to_timestamp("produced_at"))
    .withColumn("ingest_date", F.to_date("kafka_timestamp")))

# Puits : Parquet dans HDFS, avec checkpoint pour la reprise
query = (bronze.writeStream
         .format("parquet")
         .option("path", BRONZE_PATH)
         .option("checkpointLocation", CHECKPOINT)
         .partitionBy("ingest_date")
         .outputMode("append")
         .trigger(processingTime="10 seconds")
         .start())

query.awaitTermination()