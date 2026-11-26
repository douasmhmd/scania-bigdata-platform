"""Fonctions communes du pipeline ML : preparation des donnees et evaluation."""
from pyspark.ml.feature import (Imputer, StringIndexer, OneHotEncoder,
                                VectorAssembler, StandardScaler)
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql import functions as F

ID_COLS = {"event_id", "vehicle_id", "split", "label", "weight"}


def feature_columns(df):
    """Variables numeriques (capteurs + time_step) et categorielles (specifications)."""
    cats = [c for c, t in df.dtypes if t == "string" and c not in ID_COLS]
    nums = [c for c, t in df.dtypes if t in ("double", "int", "bigint") and c not in ID_COLS]
    return nums, cats


def preparation_stages(nums, cats):
    """Preparations incluses dans le modele sauvegarde (exigence de l'enonce)."""
    return [
        Imputer(inputCols=nums, outputCols=[f"{c}_imp" for c in nums], strategy="median"),
        StringIndexer(inputCols=cats, outputCols=[f"{c}_idx" for c in cats], handleInvalid="keep"),
        OneHotEncoder(inputCols=[f"{c}_idx" for c in cats],
                      outputCols=[f"{c}_ohe" for c in cats], handleInvalid="keep"),
        VectorAssembler(inputCols=[f"{c}_imp" for c in nums] + [f"{c}_ohe" for c in cats],
                        outputCol="raw_features"),
        StandardScaler(inputCol="raw_features", outputCol="features"),
    ]


def add_class_weights(df):
    """Poids equilibres : la classe panne pese autant que la classe saine au total."""
    n = df.count()
    n_pos = df.filter(F.col("label") == 1.0).count()
    w_pos, w_neg = n / (2.0 * n_pos), n / (2.0 * (n - n_pos))
    return df.withColumn("weight", F.when(F.col("label") == 1.0, w_pos).otherwise(w_neg))


def evaluate(pred, name):
    """Matrice de confusion et metriques de la classe panne (positive)."""
    m = {(r["label"], r["prediction"]): r["count"]
         for r in pred.groupBy("label", "prediction").count().collect()}
    tp, fp = m.get((1.0, 1.0), 0), m.get((0.0, 1.0), 0)
    fn, tn = m.get((1.0, 0.0), 0), m.get((0.0, 0.0), 0)
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    pr_auc = None
    if "rawPrediction" in pred.columns:
        pr_auc = round(BinaryClassificationEvaluator(metricName="areaUnderPR").evaluate(pred), 4)
    return (name, tp, fp, fn, tn, round(precision, 4), round(recall, 4), round(f1, 4), pr_auc)


RESULT_COLS = ["modele", "TP", "FP", "FN", "TN", "precision", "rappel", "f1", "pr_auc"]