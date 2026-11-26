"""
Etape ML 2 - reglage du GBT, choix du seuil, evaluation finale et sauvegarde.
1. Grille de parametres du GBT evaluee sur la validation (PR-AUC)
2. Reentrainement de la meilleure configuration sur tout l'entrainement
3. Choix du seuil de decision qui maximise le F1 sur la validation
4. Evaluation finale sur le test, jamais utilise auparavant
5. Sauvegarde du PipelineModel (preparations + modele + seuil) dans HDFS
"""
import itertools
import json

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.functions import vector_to_array
from ml_common import feature_columns, preparation_stages, add_class_weights, evaluate, RESULT_COLS

HDFS = "hdfs://namenode:8020"
GOLD = f"{HDFS}/lakehouse/gold/training"
MODEL_PATH = f"{HDFS}/models/scania_pdm/v1"
META_PATH = f"{HDFS}/models/scania_pdm/v1_metadata"
TUNE_SAMPLE = 0.2                                   # echantillon pour la grille
GRID = {"maxDepth": [4, 6], "maxIter": [30, 60]}    # parametres specifiques au GBT
HORIZON = 48

spark = SparkSession.builder.appName("ml-tuning-gbt").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

gold = spark.read.parquet(GOLD)
nums, cats = feature_columns(gold)
prep = preparation_stages(nums, cats)
train_tune = add_class_weights(gold.filter("split = 'train'")
                               .sample(fraction=TUNE_SAMPLE, seed=42)).cache()
val = gold.filter("split = 'validation'").cache()
test = gold.filter("split = 'test'")
pr_auc = BinaryClassificationEvaluator(metricName="areaUnderPR")

# 1. Grille de parametres, evaluee sur la validation
best = None
for depth, iters in itertools.product(GRID["maxDepth"], GRID["maxIter"]):
    gbt = GBTClassifier(weightCol="weight", maxDepth=depth, maxIter=iters, seed=42)
    score = pr_auc.evaluate(Pipeline(stages=prep + [gbt]).fit(train_tune).transform(val))
    print(f"GBT maxDepth={depth} maxIter={iters} -> PR-AUC validation = {score:.4f}")
    if best is None or score > best[0]:
        best = (score, depth, iters)
_, depth, iters = best
print(f"Meilleure configuration : maxDepth={depth}, maxIter={iters}")

# 2. Reentrainement sur tout l'entrainement
train_full = add_class_weights(gold.filter("split = 'train'"))
gbt = GBTClassifier(weightCol="weight", maxDepth=depth, maxIter=iters, seed=42)
model = Pipeline(stages=prep + [gbt]).fit(train_full)

# 3. Choix du seuil qui maximise le F1 sur la validation
pv = (model.transform(val)
      .select("label", vector_to_array("probability")[1].alias("p")).cache())
best_t = (0.5, -1.0)
for t in [round(0.30 + 0.05 * i, 2) for i in range(14)]:   # de 0.30 a 0.95
    r = pv.agg(F.sum(((F.col("p") >= t) & (F.col("label") == 1)).cast("int")).alias("tp"),
               F.sum(((F.col("p") >= t) & (F.col("label") == 0)).cast("int")).alias("fp"),
               F.sum(((F.col("p") < t) & (F.col("label") == 1)).cast("int")).alias("fn")).first()
    prec = r.tp / (r.tp + r.fp) if r.tp + r.fp else 0.0
    rec = r.tp / (r.tp + r.fn) if r.tp + r.fn else 0.0
    f1 = 2 * prec * rec / (prec + rec) if prec + rec else 0.0
    print(f"seuil {t:.2f} : precision {prec:.3f}  rappel {rec:.3f}  F1 {f1:.3f}")
    if f1 > best_t[1]:
        best_t = (t, f1)
threshold = best_t[0]
model.stages[-1].setThresholds([1 - threshold, threshold])   # le seuil est sauvegarde avec le modele
print(f"Seuil retenu : {threshold}")

# 4. Evaluation finale sur le test
results = [evaluate(test.withColumn("prediction", F.lit(0.0)), "Reference (toujours 0)"),
           evaluate(model.transform(test), f"GBT regle (seuil {threshold})")]
spark.createDataFrame(results, RESULT_COLS).show(truncate=False)

# 5. Sauvegarde du modele et de ses metadonnees
model.write().overwrite().save(MODEL_PATH)
meta = {"version": "v1", "algorithme": "GBTClassifier", "maxDepth": depth, "maxIter": iters,
        "seuil": threshold, "horizon_pas_de_temps": HORIZON,
        "variables_numeriques": len(nums), "variables_categorielles": len(cats),
        "metriques_test": dict(zip(RESULT_COLS, results[-1]))}
(spark.createDataFrame([(json.dumps(meta, ensure_ascii=False),)], ["metadata"])
      .coalesce(1).write.mode("overwrite").text(META_PATH))
print(json.dumps(meta, indent=2, ensure_ascii=False))
spark.stop()