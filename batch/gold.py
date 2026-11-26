"""
Job batch - couche Gold (jeu d'entrainement).
Construit le label de chaque releve a partir de train_tte, avec gestion de la
censure, puis decoupe les donnees PAR CAMION en entrainement / validation / test.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

HDFS = "hdfs://namenode:8020"
SILVER = f"{HDFS}/lakehouse/silver/readouts"
TTE = f"{HDFS}/lakehouse/reference/train_tte.csv"
GOLD = f"{HDFS}/lakehouse/gold/training"

HORIZON = 48  # fenetre de prediction en pas de temps (a confirmer dans la doc du challenge)

spark = SparkSession.builder.appName("gold-training").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

silver = spark.read.parquet(SILVER)
tte = spark.read.csv(TTE, header=True, inferSchema=True)

df = (silver.join(tte, on="vehicle_id", how="inner")
      .withColumn("time_to_end", F.col("length_of_study_time_step") - F.col("time_step")))

# Regle de labellisation avec censure
df = df.withColumn("label",
    F.when(F.col("time_to_end") < 0, None)                       # incoherent : exclu
     .when((F.col("in_study_repair") == 1) & (F.col("time_to_end") <= HORIZON), 1.0)
     .when(F.col("time_to_end") > HORIZON, 0.0)
     .otherwise(None))                                            # censure : label inconnu

n_total = df.count()
n_negatifs = df.filter(F.col("time_to_end") < 0).count()
labelled = df.filter(F.col("label").isNotNull())

# Decoupage deterministe PAR CAMION : 70 % / 15 % / 15 %
bucket = F.pmod(F.hash("vehicle_id"), 100)
labelled = labelled.withColumn("split",
    F.when(bucket < 70, "train").when(bucket < 85, "validation").otherwise("test"))

# Les informations sur le futur (fin d'etude, reparation) ne doivent JAMAIS servir de variables
gold = labelled.drop("length_of_study_time_step", "in_study_repair", "time_to_end")
gold.write.mode("overwrite").partitionBy("split").parquet(GOLD)

# Controles
g = spark.read.parquet(GOLD)
n_gold = g.count()
print(f"Releves Silver avec TTE        : {n_total}")
print(f"Releves incoherents (T - t < 0): {n_negatifs}")
print(f"Releves exclus (censure)       : {n_total - n_gold - n_negatifs}")
print(f"Releves Gold                   : {n_gold}")
(g.groupBy("split")
  .agg(F.countDistinct("vehicle_id").alias("camions"),
       F.count("*").alias("releves"),
       F.sum("label").cast("int").alias("positifs"),
       F.round(100 * F.avg("label"), 2).alias("pct_positifs"))
  .orderBy("split").show())

spark.stop()