"""
Job batch Spark SQL - premiers indicateurs (jalon de la semaine 2).
Lit l'historique Bronze dans HDFS et calcule des indicateurs de volumetrie,
de qualite des donnees et de couverture des labels.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

HDFS = "hdfs://namenode:8020"
BRONZE = f"{HDFS}/lakehouse/bronze/readouts"
TTE = f"{HDFS}/lakehouse/reference/train_tte.csv"
OUT = f"{HDFS}/lakehouse/indicators"

spark = SparkSession.builder.appName("batch-indicators-s2").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

bronze = spark.read.parquet(BRONZE)
bronze.createOrReplaceTempView("bronze")
spark.read.csv(TTE, header=True, inferSchema=True).createOrReplaceTempView("tte")

# 1. Volumetrie et doublons
volume = spark.sql("""
    SELECT COUNT(*)                            AS evenements,
           COUNT(DISTINCT event_id)            AS evenements_uniques,
           COUNT(*) - COUNT(DISTINCT event_id) AS doublons,
           COUNT(DISTINCT vehicle_id)          AS camions
    FROM bronze
""")

# 2. Releves par camion (apres dedoublonnage)
par_camion = spark.sql("""
    WITH dedup AS (SELECT DISTINCT event_id, vehicle_id, time_step FROM bronze),
         v AS (SELECT vehicle_id, COUNT(*) AS n, MIN(time_step) AS t_min, MAX(time_step) AS t_max
               FROM dedup GROUP BY vehicle_id)
    SELECT MIN(n) AS min_releves,
           ROUND(AVG(n), 1) AS moy_releves,
           PERCENTILE_APPROX(n, 0.5) AS mediane_releves,
           MAX(n) AS max_releves,
           ROUND(AVG(t_max - t_min), 1) AS duree_moy_suivi
    FROM v
""")

# 3. Labels : part des camions repares pendant l'etude
labels = spark.sql("""
    SELECT COUNT(*)                                 AS camions_tte,
           SUM(in_study_repair)                     AS camions_repares,
           ROUND(100 * AVG(in_study_repair), 2)     AS pct_repares,
           ROUND(AVG(length_of_study_time_step), 1) AS duree_moy_etude
    FROM tte
""")

# 4. Couverture : les camions de Bronze ont-ils tous un label ?
couverture = spark.sql("""
    SELECT COUNT(DISTINCT b.vehicle_id) AS camions_bronze,
           COUNT(DISTINCT t.vehicle_id) AS camions_avec_label
    FROM (SELECT DISTINCT vehicle_id FROM bronze) b
    LEFT JOIN tte t ON b.vehicle_id = t.vehicle_id
""")

# 5. Qualite : taux de valeurs manquantes par variable, en un seul passage
keys = sorted(bronze.select(F.map_keys("readings")).first()[0])
taux = bronze.agg(*[
    F.round(100 * F.avg(F.col("readings")[k].isNull().cast("int")), 2).alias(k)
    for k in keys]).first().asDict()
manquants = spark.createDataFrame(
    sorted(taux.items(), key=lambda kv: -kv[1]), ["variable", "pct_manquants"])

# Affichage
for titre, df in [("Volumetrie", volume), ("Releves par camion", par_camion),
                  ("Labels", labels), ("Couverture", couverture)]:
    print(f"\n=== {titre} ===")
    df.show(truncate=False)
print("\n=== Top 10 des variables avec le plus de valeurs manquantes ===")
manquants.show(10, truncate=False)

# Sauvegarde dans HDFS (restitution en base en semaine 4)
for nom, df in [("volumetrie", volume), ("releves_par_camion", par_camion),
                ("labels", labels), ("couverture", couverture), ("manquants", manquants)]:
    df.write.mode("overwrite").parquet(f"{OUT}/{nom}")

spark.stop()