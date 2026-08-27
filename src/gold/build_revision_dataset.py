from __future__ import annotations

import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

from src.common.config import (GOLD_ROOT, SILVER_ROOT, ConfigurationError,
                               LakeSettings)
from src.common.spark import build_session
from src.silver.model import magnitude_family, saturation_threshold

APPLICATION_NAME = "gold-revision-dataset"
VERSIONS_TABLE = f"{SILVER_ROOT}/event_versions"
DATASET_TABLE = f"{GOLD_ROOT}/magnitude_revision"

MOVEMENT_THRESHOLD = 0.05

FIRST_FEATURES = ("magnitude", "magnitude_type", "review_status", "contributor",
                  "station_count", "magnitude_station_count", "azimuthal_gap",
                  "minimum_distance", "phase_count", "standard_error",
                  "horizontal_error", "magnitude_error", "depth_km",
                  "latitude", "longitude", "minutes_since_quake")


def read_versions(session: SparkSession, settings: LakeSettings) -> DataFrame:
    return session.read.format("delta").load(f"{settings.hdfs_uri}{VERSIONS_TABLE}")


def side(versions: DataFrame, flag: str, prefix: str,
         columns: tuple[str, ...]) -> DataFrame:
    selected = [F.col(name).alias(f"{prefix}_{name}") for name in columns]
    return (versions.filter(F.col(flag))
            .select("event_id", "cohort", "place", "event_time",
                    "version_count", *selected))


def build_dataset(versions: DataFrame) -> DataFrame:
    first = side(versions, "is_first", "first", FIRST_FEATURES)
    last = side(versions, "is_last", "final",
                ("magnitude", "magnitude_type", "review_status", "contributor",
                 "station_count", "azimuthal_gap", "minutes_since_quake"))

    paired = first.join(
        last.drop("cohort", "place", "event_time", "version_count"),
        on="event_id", how="inner")

    return (paired
            .withColumn("shift", F.round(F.col("final_magnitude")
                                         - F.col("first_magnitude"), 2))
            .withColumn("shift_abs", F.abs(F.col("shift")))
            .withColumn("moved", F.col("shift_abs") >= MOVEMENT_THRESHOLD)
            .withColumn("direction",
                        F.when(F.col("shift") >= MOVEMENT_THRESHOLD, "hausse")
                         .when(F.col("shift") <= -MOVEMENT_THRESHOLD, "baisse")
                         .otherwise("stable"))
            .withColumn("first_family", magnitude_family(F.col("first_magnitude_type")))
            .withColumn("final_family", magnitude_family(F.col("final_magnitude_type")))
            .withColumn("scale_changed",
                        F.col("first_magnitude_type") != F.col("final_magnitude_type"))
            .withColumn("first_saturation",
                        saturation_threshold(F.col("first_magnitude_type")))
            .withColumn("first_near_saturation",
                        F.col("first_magnitude") >= F.col("first_saturation") - 1.0)
            .withColumn("station_growth",
                        F.col("final_station_count") - F.col("first_station_count"))
            .withColumn("review_delay_minutes",
                        F.round(F.col("final_minutes_since_quake")
                                - F.col("first_minutes_since_quake"), 1)))


def write_gold(settings: LakeSettings, dataset: DataFrame) -> None:
    (dataset.coalesce(1).write.mode("overwrite")
     .parquet(f"{settings.hdfs_uri}{DATASET_TABLE}"))


def report(dataset: DataFrame) -> None:
    total = dataset.count()
    moved = dataset.filter("moved").count()

    print("-" * 74)
    print(f"  paires premiere/finale : {total}")
    print(f"  magnitude modifiee     : {moved}  ({100.0 * moved / total:.1f} %)")
    print("-" * 74)

    dataset.groupBy("cohort").agg(
        F.count("*").alias("seismes"),
        F.round(F.avg(F.col("moved").cast("int")) * 100, 1).alias("pct_modifie"),
        F.round(F.avg("shift"), 3).alias("ecart_moyen"),
        F.round(F.expr("percentile_approx(shift_abs, 0.5)"), 2).alias("ecart_abs_median"),
        F.round(F.max("shift_abs"), 2).alias("ecart_abs_max"),
    ).show(truncate=False)

    print("  ecart moyen par famille de la premiere echelle :")
    dataset.groupBy("first_family").agg(
        F.count("*").alias("seismes"),
        F.round(F.avg("shift"), 3).alias("ecart_moyen"),
        F.round(F.avg(F.col("scale_changed").cast("int")) * 100, 1)
            .alias("pct_changement_echelle"),
    ).orderBy(F.desc("seismes")).show(truncate=False)


def main() -> int:
    try:
        settings = LakeSettings.from_environment()
        session = build_session(APPLICATION_NAME, with_delta=True)
    except ConfigurationError as error:
        print(error, file=sys.stderr)
        return 2

    try:
        versions = read_versions(session, settings)
    except AnalysisException:
        print("table Silver event_versions absente", file=sys.stderr)
        session.stop()
        return 1

    dataset = build_dataset(versions).cache()
    if dataset.isEmpty():
        print("aucune paire premiere/finale exploitable", file=sys.stderr)
        session.stop()
        return 1

    write_gold(settings, dataset)
    report(dataset)

    session.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
