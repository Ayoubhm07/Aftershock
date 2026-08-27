from __future__ import annotations

from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import (Imputer, OneHotEncoder, StringIndexer,
                                VectorAssembler)
from pyspark.ml.regression import GBTRegressor
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.ml.features import (CATEGORICAL_FEATURES, TARGET, indicator_names,
                             usable_numeric)

MOVEMENT_THRESHOLD = 0.05


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
    # Perte absolue : la metrique de jugement est la MAE. Entrainer sur une
    # perte quadratique reviendrait a optimiser autre chose que ce qu'on mesure.
    model = GBTRegressor(featuresCol="features", labelCol=TARGET,
                         lossType="absolute", maxIter=80, maxDepth=3,
                         stepSize=0.06, subsamplingRate=0.8, seed=seed)
    return Pipeline(stages=[*indexers, *encoders, imputer, assembler, model])


def score(predictions: DataFrame, column: str) -> dict:
    return {name: RegressionEvaluator(labelCol=TARGET, predictionCol=column,
                                      metricName=name).evaluate(predictions)
            for name in ("mae", "rmse")}


def baselines(train: DataFrame, test: DataFrame) -> dict:
    average = train.agg(F.avg(TARGET)).first()[0] or 0.0
    candidates = (test
                  .withColumn("zero", F.lit(0.0))
                  .withColumn("mean", F.lit(float(average))))
    mean_score = score(candidates, "mean")
    mean_score["valeur"] = round(float(average), 4)
    return {"toujours_zero": score(candidates, "zero"),
            "moyenne_apprentissage": mean_score}


def sign_agreement(predictions: DataFrame) -> dict:
    moved = predictions.filter(F.abs(F.col(TARGET)) >= MOVEMENT_THRESHOLD)
    total = moved.count()
    if not total:
        return {"seismes_deplaces": 0, "signe_correct_pct": None}
    agreed = moved.filter(
        F.signum(F.col("prediction")) == F.signum(F.col(TARGET))).count()
    return {"seismes_deplaces": total,
            "signe_correct_pct": round(100.0 * agreed / total, 1)}


def residual_band(predictions: DataFrame) -> dict:
    residuals = predictions.withColumn(
        "residual", F.abs(F.col(TARGET) - F.col("prediction")))
    quantiles = residuals.selectExpr(
        "percentile_approx(residual, 0.5) AS p50",
        "percentile_approx(residual, 0.8) AS p80",
        "percentile_approx(residual, 0.9) AS p90").first()
    return {name: round(float(quantiles[name]), 3)
            for name in ("p50", "p80", "p90")}


def importances(fitted) -> list[dict]:
    names = fitted.stages[-2].getInputCols()
    weights = fitted.stages[-1].featureImportances.toArray()

    grouped: dict[str, float] = {}
    for name, weight in zip(names, weights):
        key = name.replace("_filled", "").replace("_vector", "")
        grouped[key] = grouped.get(key, 0.0) + float(weight)

    ranked = sorted(grouped.items(), key=lambda item: -item[1])
    return [{"variable": key, "poids": round(value, 4)}
            for key, value in ranked if value > 0]


def run(train: DataFrame, test: DataFrame, seed: int) -> tuple[dict, object]:
    numeric = usable_numeric(train)
    fitted = stages(numeric, seed).fit(train)
    predictions = fitted.transform(test).cache()

    measured = score(predictions, "prediction")
    reference = baselines(train, test)
    zero_mae = reference["toujours_zero"]["mae"]

    report = {
        "question": "de combien la magnitude va-t-elle bouger",
        "variables_numeriques": numeric,
        "modele": measured,
        "references": reference,
        "accord_de_signe": sign_agreement(predictions),
        "bande_de_residus": residual_band(predictions),
        "importances": importances(fitted),
        "gain_sur_reference_pct": (
            round(100.0 * (zero_mae - measured["mae"]) / zero_mae, 1)
            if zero_mae else 0.0),
        "bat_la_reference": measured["mae"] < zero_mae,
    }
    return report, fitted
