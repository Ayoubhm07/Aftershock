from __future__ import annotations

import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

from src.common.config import (GOLD_ROOT, SILVER_ROOT, ConfigurationError,
                               LakeSettings)
from src.common.spark import build_session

APPLICATION_NAME = "gold-witness"
EVENTS_TABLE = f"{SILVER_ROOT}/events"
REVISION_TABLE = f"{GOLD_ROOT}/magnitude_revision"
WITNESS_TABLE = f"{GOLD_ROOT}/human_witness"

MINIMUM_REPORTS = 5
SIGNIFICANT_SHIFT = 0.2


def read_events(session: SparkSession, settings: LakeSettings) -> DataFrame:
    return (session.read.format("delta")
            .load(f"{settings.hdfs_uri}{EVENTS_TABLE}")
            .filter(F.col("is_current"))
            .select("usgs_id", "place", "felt_reports", "community_intensity",
                    "instrumental_intensity", "alert_level", "tsunami_flag"))


def read_revisions(session: SparkSession, settings: LakeSettings) -> DataFrame:
    return (session.read.parquet(f"{settings.hdfs_uri}{REVISION_TABLE}")
            .select("event_id", "first_magnitude", "first_magnitude_type",
                    "first_station_count", "final_magnitude", "shift",
                    "shift_abs", "direction", "cohort"))


def build_witness(events: DataFrame, revisions: DataFrame) -> DataFrame:
    joined = revisions.join(
        events, revisions.event_id == events.usgs_id, "inner").drop("usgs_id")

    return (joined
            .withColumn("has_witness", F.col("felt_reports") >= MINIMUM_REPORTS)
            .withColumn("intensity_gap",
                        F.round(F.col("community_intensity")
                                - F.col("instrumental_intensity"), 2))
            .withColumn("witness_louder", F.col("intensity_gap") > 0)
            .withColumn("moved_up", F.col("shift") >= SIGNIFICANT_SHIFT)
            .withColumn("moved_down", F.col("shift") <= -SIGNIFICANT_SHIFT))


def report(witness: DataFrame) -> None:
    total = witness.count()
    heard = witness.filter("has_witness").count()

    print("-" * 76)
    print(f"  seismes apparies avec le catalogue : {total}")
    print(f"  dont au moins {MINIMUM_REPORTS} temoignages humains : {heard}"
          + (f"  ({100.0 * heard / total:.1f} %)" if total else ""))
    print("-" * 76)

    if not heard:
        print("  Aucun seisme ne porte assez de temoignages : la troisieme")
        print("  source existe dans le schema mais reste vide sur ce lot.")
        print("  C'est un constat, pas un echec de traitement : les seismes")
        print("  M>=4 sont majoritairement oceaniques et personne ne les sent.")
        return

    print("  La revision est-elle plus frequente quand des humains ont senti ?")
    witness.groupBy("has_witness").agg(
        F.count("*").alias("seismes"),
        F.round(F.avg(F.col("moved_up").cast("int")) * 100, 1).alias("pct_hausse"),
        F.round(F.avg(F.col("moved_down").cast("int")) * 100, 1).alias("pct_baisse"),
        F.round(F.avg("shift_abs"), 3).alias("ecart_absolu_moyen"),
    ).orderBy("has_witness").show(truncate=False)

    comparable = witness.filter("has_witness AND intensity_gap IS NOT NULL")
    if comparable.count() >= 10:
        print("  Quand le ressenti humain depasse la mesure instrumentale,")
        print("  la magnitude est-elle relevee ensuite ?")
        comparable.groupBy("witness_louder").agg(
            F.count("*").alias("seismes"),
            F.round(F.avg(F.col("moved_up").cast("int")) * 100, 1).alias("pct_hausse"),
            F.round(F.avg("shift"), 3).alias("ecart_moyen"),
            F.round(F.avg("intensity_gap"), 2).alias("ecart_intensite_moyen"),
        ).orderBy("witness_louder").show(truncate=False)
    else:
        print(f"  {comparable.count()} seismes ont a la fois un ressenti humain")
        print("  et une mesure instrumentale : trop peu pour conclure.")


def main() -> int:
    try:
        settings = LakeSettings.from_environment()
        session = build_session(APPLICATION_NAME, with_delta=True)
    except ConfigurationError as error:
        print(error, file=sys.stderr)
        return 2

    try:
        events = read_events(session, settings)
        revisions = read_revisions(session, settings)
    except AnalysisException as error:
        print(f"table absente : {error}", file=sys.stderr)
        session.stop()
        return 1

    witness = build_witness(events, revisions).cache()
    if witness.isEmpty():
        print("aucun appariement entre le catalogue et les revisions",
              file=sys.stderr)
        session.stop()
        return 1

    (witness.coalesce(1).write.mode("overwrite")
     .parquet(f"{settings.hdfs_uri}{WITNESS_TABLE}"))

    report(witness)
    session.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
