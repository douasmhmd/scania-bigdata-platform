"""
Etape ML 1 - comparaison de trois techniques (jalon semaine 3).
Entraine LR, RF et GBT sur l'entrainement et les compare sur la validation,
face a une reference naive (toujours "pas de panne").
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier, GBTClassifier
from ml_common import feature_columns, preparation_stages, add_class_weights, evaluate, RESULT_COLS

HDFS = "hdfs://namenode:8020"
GOLD = f"{HDFS}/lakehouse/gold/training"
OUT = f"{HDFS}/lakehouse/ml/comparaison"
SAMPLE = 0.2   # fraction de l'entrainement : 0.2 pour valider le code, puis 1.0

spark = SparkSession.builder.appName("ml-comparaison").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

gold = spark.read.parquet(GOLD)
train = gold.filter("split = 'train'")
if SAMPLE < 1.0:
    train = train.sample(fraction=SAMPLE, seed=42)
train = add_class_weights(train).cache()
val = gold.filter("split = 'validation'").cache()

nums, cats = feature_columns(gold)
print(f"Variables : {len(nums)} numeriques, {len(cats)} categorielles")
prep = preparation_stages(nums, cats)

models = {
    "LogisticRegression": LogisticRegression(weightCol="weight", maxIter=50, regParam=0.01),
    "RandomForest": RandomForestClassifier(weightCol="weight", numTrees=50, maxDepth=8, seed=42),
    "GBT": GBTClassifier(weightCol="weight", maxIter=30, maxDepth=5, seed=42),
}

results = [evaluate(val.withColumn("prediction", F.lit(0.0)), "Reference (toujours 0)")]
for name, clf in models.items():
    model = Pipeline(stages=prep + [clf]).fit(train)
    results.append(evaluate(model.transform(val), name))
    print(f"Termine : {name}")

table = spark.createDataFrame(results, RESULT_COLS)
table.show(truncate=False)
table.write.mode("overwrite").json(OUT)
spark.stop()