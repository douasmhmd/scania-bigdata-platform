"""
Job batch - couche Silver.
Nettoie l'historique Bronze : suppression des evenements invalides et des
doublons, mise a plat des 105 variables, jointure des specifications.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

HDFS = "hdfs://namenode:8020"
BRONZE = f"{HDFS}/lakehouse/bronze/readouts"
SPECS = f"{HDFS}/lakehouse/reference/train_specifications.csv"
SILVER = f"{HDFS}/lakehouse/silver/readouts"

spark = SparkSession.builder.appName("silver-readouts").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

bronze = spark.read.parquet(BRONZE)
n_bronze = bronze.count()

# 1. Validite : on ecarte les evenements sans identifiant ou sans temps
valid = bronze.filter(F.col("event_id").isNotNull()
                      & F.col("vehicle_id").isNotNull()
                      & F.col("time_step").isNotNull())
n_valid = valid.count()

# 2. Dedoublonnage : un evenement = un event_id
dedup = valid.dropDuplicates(["event_id"])

# 3. Mise a plat des 105 variables (une colonne par variable)
keys = sorted(bronze.select(F.map_keys("readings")).first()[0])
flat = dedup.select("event_id", "vehicle_id", "time_step",
                    *[F.col("readings")[k].alias(k) for k in keys])

# 4. Jointure des specifications du camion
specs = spark.read.csv(SPECS, header=True, inferSchema=True)
silver = flat.join(specs, on="vehicle_id", how="left")

silver.write.mode("overwrite").parquet(SILVER)

# Controles de qualite sur la table ecrite
s = spark.read.parquet(SILVER)
n_silver = s.count()
spec_cols = [c for c in specs.columns if c != "vehicle_id"]
n_sans_specs = s.filter(F.col(spec_cols[0]).isNull()).count()

print(f"Bronze              : {n_bronze}")
print(f"Invalides ecartes   : {n_bronze - n_valid}")
print(f"Doublons ecartes    : {n_valid - n_silver}")
print(f"Silver              : {n_silver} lignes, "
      f"{s.select('vehicle_id').distinct().count()} camions, {len(s.columns)} colonnes")
print(f"Sans specifications : {n_sans_specs}")

spark.stop()