from __future__ import annotations

import argparse
import sys

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

from src.common.config import (GOLD_ROOT, SILVER_ROOT, ConfigurationError,
                               LakeSettings)
from src.common.spark import build_session

APPLICATION_NAME = "gold-time-travel"
EVENTS_TABLE = f"{SILVER_ROOT}/events"
DIFF_TABLE = f"{GOLD_ROOT}/lake_diff"

COMPARED = ("magnitude", "magnitude_type", "review_status", "usgs_id")


def lake_history(session: SparkSession, target: str) -> DataFrame:
    return (DeltaTable.forPath(session, target).history()
            .select(
                "version",
                F.col("timestamp").alias("commit_time"),
                "operation",
                F.col("operationMetrics.numOutputRows").cast("long").alias("lignes"),
                F.col("operationMetrics.numTargetRowsInserted").cast("long")
                    .alias("inserees"),
                F.col("operationMetrics.numTargetRowsUpdated").cast("long")
                    .alias("mises_a_jour"))
            .orderBy(F.desc("version")))


def snapshot(session: SparkSession, target: str, version: int) -> DataFrame:
    return (session.read.format("delta")
            .option("versionAsOf", version)
            .load(target)
            .filter(F.col("is_current"))
            .select("event_key", "place", "event_time", *COMPARED))


def compare(before: DataFrame, after: DataFrame) -> DataFrame:
    left = before.select(
        "event_key", *[F.col(name).alias(f"avant_{name}") for name in COMPARED])
    right = after.select(
        "event_key", "place", "event_time",
        *[F.col(name).alias(f"apres_{name}") for name in COMPARED])

    joined = right.join(left, on="event_key", how="full_outer")

    changed = [F.col(f"avant_{name}").eqNullSafe(F.col(f"apres_{name}"))
               for name in COMPARED]
    identical = changed[0]
    for expression in changed[1:]:
        identical = identical & expression

    return (joined
            .withColumn("statut",
                        F.when(F.col("avant_usgs_id").isNull(), "apparu")
                         .when(F.col("apres_usgs_id").isNull(), "disparu")
                         .when(identical, "inchange")
                         .otherwise("modifie"))
            .withColumn("ecart_magnitude",
                        F.round(F.col("apres_magnitude")
                                - F.col("avant_magnitude"), 2))
            .filter(F.col("statut") != "inchange"))


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Compare deux versions du lac Silver.")
    parser.add_argument("--before", type=int, default=None)
    parser.add_argument("--after", type=int, default=None)
    parser.add_argument("--write", action="store_true")
    arguments = parser.parse_args()

    try:
        settings = LakeSettings.from_environment()
        session = build_session(APPLICATION_NAME, with_delta=True)
    except ConfigurationError as error:
        print(error, file=sys.stderr)
        return 2

    target = f"{settings.hdfs_uri}{EVENTS_TABLE}"
    try:
        history = lake_history(session, target)
    except (AnalysisException, Exception) as error:
        print(f"table Delta illisible : {error}", file=sys.stderr)
        session.stop()
        return 1

    print("  histoire du lac Silver :")
    history.show(20, truncate=False)

    versions = [row["version"] for row in history.collect()]
    if len(versions) < 2:
        print("une seule version : rien a comparer", file=sys.stderr)
        session.stop()
        return 1

    after = arguments.after if arguments.after is not None else max(versions)
    before = arguments.before if arguments.before is not None else after - 1

    if before not in versions or after not in versions:
        print(f"versions demandees hors de {sorted(versions)}", file=sys.stderr)
        session.stop()
        return 1

    print(f"  comparaison : version {before} -> version {after}")
    difference = compare(snapshot(session, target, before),
                         snapshot(session, target, after)).cache()

    total = difference.count()
    print(f"  {total} evenement(s) different(s) entre les deux versions")

    if total:
        difference.groupBy("statut").count().orderBy(F.desc("count")).show(
            truncate=False)
        print("  les plus gros ecarts de magnitude :")
        (difference.filter(F.col("ecart_magnitude").isNotNull())
         .filter(F.abs(F.col("ecart_magnitude")) > 0)
         .select("event_key", "place", "avant_magnitude", "apres_magnitude",
                 "ecart_magnitude", "avant_usgs_id", "apres_usgs_id")
         .orderBy(F.desc(F.abs(F.col("ecart_magnitude"))))
         .show(10, truncate=False))

        if arguments.write:
            (difference
             .withColumn("version_avant", F.lit(before))
             .withColumn("version_apres", F.lit(after))
             .coalesce(1).write.mode("overwrite")
             .parquet(f"{settings.hdfs_uri}{DIFF_TABLE}"))
            print(f"  ecrit dans {DIFF_TABLE}")

    session.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
