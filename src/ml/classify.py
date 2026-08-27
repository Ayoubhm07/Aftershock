from __future__ import annotations

from pyspark.ml import Pipeline
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import (Imputer, OneHotEncoder, StringIndexer,
                                VectorAssembler)
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.ml.features import (CATEGORICAL_FEATURES, TARGET, indicator_names,
                             usable_numeric)

# Un dixieme de magnitude est du bruit de mesure ; deux dixiemes deplacent une
# decision. C'est ce seuil-la que le modele doit apprendre a annoncer.
SIGNIFICANT_SHIFT = 0.2
LABEL = "sous_estime"


def add_label(dataset: DataFrame) -> DataFrame:
    return dataset.withColumn(
        LABEL, (F.col(TARGET) >= SIGNIFICANT_SHIFT).cast("double"))


def stages(numeric: list[str], seed: int) -> Pipeline:
    indexers = [StringIndexer(inputCol=name, outputCol=f"{name}_index",
                              handleInvalid="keep")
                for name in CATEGORICAL_FEATURES]
    encoders = [OneHotEncoder(inputCol=f"{name}_index",
                              outputCol=f"{name}_vector", handleInvalid="keep")
                for name in CATEGORICAL_FEATURES]
    imputer = Imputer(strategy="median", inputCols=numeric,
                      outputCols=[f"{name}_filled" for name in numeric])
    assembler = VectorAssembler(
        inputCols=[f"{name}_filled" for name in numeric]
        + list(indicator_names())
        + [f"{name}_vector" for name in CATEGORICAL_FEATURES],
        outputCol="features", handleInvalid="keep")
    model = GBTClassifier(featuresCol="features", labelCol=LABEL,
                          maxIter=80, maxDepth=3, stepSize=0.06,
                          subsamplingRate=0.8, seed=seed)
    return Pipeline(stages=[*indexers, *encoders, imputer, assembler, model])


def confusion(predictions: DataFrame) -> dict:
    counts = predictions.groupBy(LABEL, "prediction").count().collect()
    table = {(int(row[LABEL]), int(row["prediction"])): row["count"]
             for row in counts}
    tp = table.get((1, 1), 0)
    fp = table.get((0, 1), 0)
    fn = table.get((1, 0), 0)
    tn = table.get((0, 0), 0)

    precision = tp / (tp + fp) if tp + fp else None
    recall = tp / (tp + fn) if tp + fn else None
    f1 = (2 * precision * recall / (precision + recall)
          if precision and recall else None)

    return {
        "vrais_positifs": tp, "faux_positifs": fp,
        "faux_negatifs": fn, "vrais_negatifs": tn,
        "precision_pct": round(100.0 * precision, 1) if precision else None,
        "rappel_pct": round(100.0 * recall, 1) if recall else None,
        "f1_pct": round(100.0 * f1, 1) if f1 else None,
    }


def majority_baseline(train: DataFrame, test: DataFrame) -> dict:
    positives = train.filter(F.col(LABEL) == 1.0).count()
    total = train.count()
    share = positives / total if total else 0.0

    test_positives = test.filter(F.col(LABEL) == 1.0).count()
    test_total = test.count()

    return {
        "classe_majoritaire": "stable" if share < 0.5 else "sous-estime",
        "part_positive_apprentissage_pct": round(100.0 * share, 1),
        "part_positive_test_pct": (round(100.0 * test_positives / test_total, 1)
                                   if test_total else None),
        "exactitude_si_toujours_stable_pct": (
            round(100.0 * (test_total - test_positives) / test_total, 1)
            if test_total else None),
    }


def run(train: DataFrame, test: DataFrame, seed: int) -> tuple[dict, object]:
    labelled_train = add_label(train)
    labelled_test = add_label(test)

    positives = labelled_train.filter(F.col(LABEL) == 1.0).count()
    if positives < 20:
        return {
            "question": (f"la magnitude sera-t-elle relevee d'au moins "
                         f"{SIGNIFICANT_SHIFT}"),
            "abandonne": f"{positives} exemples positifs, insuffisant",
        }, None

    numeric = usable_numeric(labelled_train)
    fitted = stages(numeric, seed).fit(labelled_train)
    predictions = fitted.transform(labelled_test).cache()

    auc = BinaryClassificationEvaluator(
        labelCol=LABEL, rawPredictionCol="rawPrediction",
        metricName="areaUnderROC").evaluate(predictions)

    return {
        "question": (f"la magnitude sera-t-elle relevee d'au moins "
                     f"{SIGNIFICANT_SHIFT}"),
        "seuil": SIGNIFICANT_SHIFT,
        "aire_sous_roc": round(float(auc), 4),
        "matrice": confusion(predictions),
        "reference": majority_baseline(labelled_train, labelled_test),
        "positifs_apprentissage": positives,
    }, fitted
