"""
Job batch - export vers PostgreSQL (restitution).
Copie les indicateurs Spark SQL, la comparaison des modeles et les metriques
du modele final depuis HDFS vers PostgreSQL, pour le tableau de bord.
"""
import json
import os

from pyspark.sql import SparkSession

HDFS = "hdfs://namenode:8020"
JDBC_URL = "jdbc:postgresql://postgres:5432/scania"
PROPS = {"user": "scania", "password": os.environ["POSTGRES_PASSWORD"],
         "driver": "org.postgresql.Driver"}

TABLES = {
    "indicateurs_volumetrie": ("parquet", f"{HDFS}/lakehouse/indicators/volumetrie"),
    "indicateurs_releves_par_camion": ("parquet", f"{HDFS}/lakehouse/indicators/releves_par_camion"),
    "indicateurs_labels": ("parquet", f"{HDFS}/lakehouse/indicators/labels"),
    "indicateurs_couverture": ("parquet", f"{HDFS}/lakehouse/indicators/couverture"),
    "indicateurs_manquants": ("parquet", f"{HDFS}/lakehouse/indicators/manquants"),
    "ml_comparaison": ("json", f"{HDFS}/lakehouse/ml/comparaison"),
}

spark = SparkSession.builder.appName("export-indicateurs").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# 1. Indicateurs et comparaison des modeles
for table, (fmt, path) in TABLES.items():
    df = spark.read.format(fmt).load(path)
    df.write.mode("overwrite").jdbc(JDBC_URL, table, properties=PROPS)
    print(f"{table} : {df.count()} lignes")

# 2. Metriques du modele final (metadonnees sauvegardees avec le modele)
meta = json.loads(spark.read.text(f"{HDFS}/models/scania_pdm/v1_metadata").first()[0])
m = meta["metriques_test"]
row = [(meta["version"], meta["algorithme"], meta["maxDepth"], meta["maxIter"], meta["seuil"],
        m["TP"], m["FP"], m["FN"], m["TN"], m["precision"], m["rappel"], m["f1"], m["pr_auc"])]
cols = ["version", "algorithme", "max_depth", "max_iter", "seuil",
        "tp", "fp", "fn", "tn", "precision", "rappel", "f1", "pr_auc"]
spark.createDataFrame(row, cols).write.mode("overwrite").jdbc(JDBC_URL, "ml_modele", properties=PROPS)
print("ml_modele : 1 ligne")

spark.stop()