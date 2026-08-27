from __future__ import annotations

import json
import sys
from datetime import datetime, timezone

from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import (Imputer, OneHotEncoder, StringIndexer,
                                VectorAssembler)
from pyspark.ml.regression import GBTRegressor
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

from src.common.config import (GOLD_ROOT, ConfigurationError, LakeSettings,
                               optional)
from src.common.hdfs import WebHdfs
from src.common.spark import build_session
from src.ml.features import (CATEGORICAL_FEATURES, NUMERIC_FEATURES, TARGET,
                             assert_no_leakage, indicator_names,
                             missing_indicators, split_by_time)

APPLICATION_NAME = "ml-revision-model"
DATASET_TABLE = f"{GOLD_ROOT}/magnitude_revision"
MODEL_ROOT = "/lake/models/revision_shift"
METRICS_TABLE = f"{GOLD_ROOT}/model_report"

MOVEMENT_THRESHOLD = 0.05
MINIMUM_TEST_SIZE = 30


def usable_numeric(train: DataFrame) -> list[str]:
    counts = train.agg(*[F.count(name).alias(name)
                         for name in NUMERIC_FEATURES]).first().asDict()
    return [name for name in NUMERIC_FEATURES if counts[name] > 0]


def build_pipeline(numeric: list[str], seed: int) -> Pipeline:
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
    model = GBTRegressor(featuresCol="features", labelCol=TARGET,
                         maxIter=120, maxDepth=4, stepSize=0.05,
                         subsamplingRate=0.8, seed=seed)
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
    model = fitted.stages[-1]
    names = fitted.stages[-2].getInputCols()
    weights = model.featureImportances.toArray()

    grouped: dict[str, float] = {}
    for name, weight in zip(names, weights):
        key = name.replace("_filled", "").replace("_vector", "")
        grouped[key] = grouped.get(key, 0.0) + float(weight)

    ranked = sorted(grouped.items(), key=lambda item: -item[1])
    return [{"variable": key, "poids": round(value, 4)}
            for key, value in ranked if value > 0]


def main() -> int:
    try:
        assert_no_leakage()
        settings = LakeSettings.from_environment()
        session = build_session(APPLICATION_NAME)
    except ConfigurationError as error:
        print(error, file=sys.stderr)
        return 2

    seed = int(optional("MODEL_SEED", "20260827"))

    try:
        dataset = session.read.parquet(f"{settings.hdfs_uri}{DATASET_TABLE}")
    except AnalysisException:
        print("table Gold magnitude_revision absente", file=sys.stderr)
        session.stop()
        return 1

    prepared = missing_indicators(dataset).cache()
    train, test, frontier = split_by_time(prepared)
    train_size, test_size = train.count(), test.count()

    print("-" * 74)
    print(f"  echantillon total   : {prepared.count()}")
    print(f"  coupure temporelle  : {frontier}")
    print(f"  apprentissage       : {train_size}    test : {test_size}")
    print("-" * 74)

    if test_size < MINIMUM_TEST_SIZE:
        print(f"ECHEC : {test_size} exemples de test, insuffisant pour conclure",
              file=sys.stderr)
        session.stop()
        return 1

    numeric = usable_numeric(train)
    dropped = sorted(set(NUMERIC_FEATURES) - set(numeric))
    if dropped:
        print(f"  ecartees, vides a l'apprentissage : {dropped}")

    fitted = build_pipeline(numeric, seed).fit(train)
    predictions = fitted.transform(test).cache()

    model_score = score(predictions, "prediction")
    reference = baselines(train, test)
    zero_mae = reference["toujours_zero"]["mae"]

    report = {
        "genere_le": datetime.now(timezone.utc).isoformat(),
        "graine": seed,
        "coupure_temporelle": frontier,
        "taille_apprentissage": train_size,
        "taille_test": test_size,
        "variables_numeriques": numeric,
        "variables_categorielles": list(CATEGORICAL_FEATURES),
        "modele": model_score,
        "references": reference,
        "accord_de_signe": sign_agreement(predictions),
        "bande_de_residus": residual_band(predictions),
        "importances": importances(fitted),
        "gain_sur_reference_pct": (
            round(100.0 * (zero_mae - model_score["mae"]) / zero_mae, 1)
            if zero_mae else 0.0),
        "bat_la_reference": model_score["mae"] < zero_mae,
    }

    fitted.write().overwrite().save(f"{settings.hdfs_uri}{MODEL_ROOT}")
    WebHdfs(settings.webhdfs_url).mkdirs(METRICS_TABLE)
    WebHdfs(settings.webhdfs_url).write(
        f"{METRICS_TABLE}/report.json",
        json.dumps(report, indent=2, ensure_ascii=False).encode("utf-8"))

    print(f"  MAE modele          : {model_score['mae']:.4f}")
    print(f"  MAE toujours zero   : {zero_mae:.4f}")
    print(f"  MAE moyenne         : "
          f"{reference['moyenne_apprentissage']['mae']:.4f}")
    print(f"  gain sur reference  : {report['gain_sur_reference_pct']} %")
    print(f"  bat la reference    : {report['bat_la_reference']}")
    print(f"  bande de residus    : {report['bande_de_residus']}")
    print(f"  accord de signe     : {report['accord_de_signe']}")
    print()
    print("  variables les plus explicatives :")
    for row in report["importances"][:10]:
        print(f"    {row['variable']:34s} {row['poids']:.4f}")

    session.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
