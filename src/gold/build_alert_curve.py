from __future__ import annotations

import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

from src.common.config import GOLD_ROOT, ConfigurationError, LakeSettings
from src.common.spark import build_session

APPLICATION_NAME = "gold-alert-curve"
DATASET_TABLE = f"{GOLD_ROOT}/magnitude_revision"
CURVE_TABLE = f"{GOLD_ROOT}/alert_curve"

THRESHOLDS = [round(3.0 + 0.1 * step, 1) for step in range(0, 41)]


def read_dataset(session: SparkSession, settings: LakeSettings) -> DataFrame:
    return session.read.parquet(f"{settings.hdfs_uri}{DATASET_TABLE}")


def build_curve(session: SparkSession, dataset: DataFrame) -> DataFrame:
    thresholds = session.createDataFrame(
        [(value,) for value in THRESHOLDS], "threshold double")

    paired = dataset.select(
        F.col("first_magnitude").alias("announced"),
        F.col("final_magnitude").alias("confirmed"))

    crossed = paired.crossJoin(F.broadcast(thresholds))

    alerted = F.col("announced") >= F.col("threshold")
    deserved = F.col("confirmed") >= F.col("threshold")

    return (crossed
            .groupBy("threshold")
            .agg(
                F.count("*").alias("seismes"),
                F.sum(F.when(alerted & deserved, 1).otherwise(0))
                    .alias("alerte_justifiee"),
                F.sum(F.when(alerted & ~deserved, 1).otherwise(0))
                    .alias("fausse_alerte"),
                F.sum(F.when(~alerted & deserved, 1).otherwise(0))
                    .alias("alerte_manquee"),
                F.sum(F.when(~alerted & ~deserved, 1).otherwise(0))
                    .alias("silence_justifie"))
            .withColumn("alertes_emises",
                        F.col("alerte_justifiee") + F.col("fausse_alerte"))
            .withColumn("taux_fausse_alerte_pct",
                        F.when(F.col("alertes_emises") > 0,
                               F.round(100.0 * F.col("fausse_alerte")
                                       / F.col("alertes_emises"), 2))
                         .otherwise(None))
            .withColumn("meritaient_alerte",
                        F.col("alerte_justifiee") + F.col("alerte_manquee"))
            .withColumn("taux_manque_pct",
                        F.when(F.col("meritaient_alerte") > 0,
                               F.round(100.0 * F.col("alerte_manquee")
                                       / F.col("meritaient_alerte"), 2))
                         .otherwise(None))
            .orderBy("threshold"))


def main() -> int:
    try:
        settings = LakeSettings.from_environment()
        session = build_session(APPLICATION_NAME)
    except ConfigurationError as error:
        print(error, file=sys.stderr)
        return 2

    try:
        dataset = read_dataset(session, settings)
    except AnalysisException:
        print("table Gold magnitude_revision absente", file=sys.stderr)
        session.stop()
        return 1

    total = dataset.count()
    curve = build_curve(session, dataset).cache()

    (curve.coalesce(1).write.mode("overwrite")
     .parquet(f"{settings.hdfs_uri}{CURVE_TABLE}"))

    print("-" * 78)
    print(f"  courbe calculee sur {total} seismes apparies")
    print("-" * 78)

    curve.filter(F.col("threshold").isin([4.0, 4.5, 5.0, 5.5, 6.0, 6.5])).select(
        "threshold", "alertes_emises", "fausse_alerte", "taux_fausse_alerte_pct",
        "alerte_manquee", "taux_manque_pct").show(truncate=False)

    session.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
