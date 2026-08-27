from __future__ import annotations

import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException
from pyspark.sql.window import Window
from pyspark.storagelevel import StorageLevel

from src.common.config import (BRONZE_ROOT, INGESTED_MARKER, SILVER_ROOT,
                               SOURCE_VERSIONS, ConfigurationError, LakeSettings)
from src.common.hdfs import WebHdfs
from src.common.spark import build_session
from src.silver.versions import VERSIONS_DOCUMENT, origin_columns

APPLICATION_NAME = "silver-version-history"
VERSIONS_TABLE = f"{SILVER_ROOT}/event_versions"

MINIMUM_MAGNITUDE = 0.0


def completed_batches(settings: LakeSettings) -> list[str]:
    fs = WebHdfs(settings.webhdfs_url)
    root = f"{BRONZE_ROOT}/source={SOURCE_VERSIONS}"
    ready = []
    for batch in sorted(fs.listdir(root)):
        directory = f"{root}/{batch}"
        if INGESTED_MARKER in fs.listdir(directory):
            ready.append(f"{settings.hdfs_uri}{directory}/versions.jsonl")
    return ready


def read_bronze(session: SparkSession, settings: LakeSettings) -> DataFrame:
    paths = completed_batches(settings)
    if not paths:
        raise AnalysisException("aucun lot marque _SUCCESS")
    print(f"  {len(paths)} lot(s) complet(s) retenu(s)")
    return session.read.schema(VERSIONS_DOCUMENT).json(paths)


def explode_origins(documents: DataFrame) -> DataFrame:
    exploded = (documents
                .filter(F.col("event_id").isNotNull())
                .select("event_id", "cohort", "event_time", "place",
                        "current_magnitude", "current_magnitude_type",
                        F.explode("products.origin").alias("origin")))

    return (exploded
            .select(
                "event_id", "cohort", "place",
                F.timestamp_millis("event_time").alias("event_time"),
                "current_magnitude", "current_magnitude_type",
                F.col("origin.source").alias("contributor"),
                F.col("origin.code").alias("contributor_code"),
                F.lower(F.col("origin.status")).alias("product_status"),
                F.col("origin.updateTime").alias("published_epoch_ms"),
                F.timestamp_millis(F.col("origin.updateTime")).alias("published_at"),
                *origin_columns())
            .filter(F.col("magnitude").isNotNull())
            .filter(F.col("product_status") != "delete"))


def rank_versions(origins: DataFrame) -> DataFrame:
    timeline = Window.partitionBy("event_id").orderBy(
        F.col("published_epoch_ms").asc(), F.col("contributor_code").asc())
    whole = timeline.rowsBetween(Window.unboundedPreceding,
                                 Window.unboundedFollowing)

    # Chaque expression de fenetre est calculee UNE fois, dans un seul select.
    # Les enchainer par withColumn en re-derivant lag() ou version_count fait
    # exploser le plan : mesure a 17 minutes sur 2 000 lignes avant correction.
    ranked = origins.select(
        "*",
        F.row_number().over(timeline).alias("version_rank"),
        F.count("*").over(whole).alias("version_count"),
        F.lag("magnitude").over(timeline).alias("previous_magnitude"),
    )

    return ranked.select(
        "*",
        (F.col("version_rank") == 1).alias("is_first"),
        (F.col("version_rank") == F.col("version_count")).alias("is_last"),
        F.round(F.col("magnitude") - F.col("previous_magnitude"), 2)
            .alias("magnitude_step"),
        F.round((F.col("published_epoch_ms") / 1000.0
                 - F.unix_timestamp("event_time")) / 60.0, 2)
            .alias("minutes_since_quake"),
    )


def write_silver(session: SparkSession, settings: LakeSettings,
                 versions: DataFrame) -> None:
    target = f"{settings.hdfs_uri}{VERSIONS_TABLE}"
    (versions.write.format("delta")
     .partitionBy("cohort")
     .mode("overwrite")
     .option("overwriteSchema", "true")
     .save(target))


def main() -> int:
    try:
        settings = LakeSettings.from_environment()
        session = build_session(APPLICATION_NAME, with_delta=True)
    except ConfigurationError as error:
        print(error, file=sys.stderr)
        return 2

    try:
        documents = read_bronze(session, settings)
    except AnalysisException:
        print("aucun lot d'historique en Bronze ; lancer versions_to_bronze",
              file=sys.stderr)
        session.stop()
        return 1

    # Les 19 lectures de la Map JSON sont materialisees avant les fenetres :
    # sinon chaque shuffle rejoue le decodage du document pour chaque champ.
    origins = explode_origins(documents).persist(StorageLevel.MEMORY_AND_DISK)
    print(f"  {origins.count()} versions brutes lues")

    versions = rank_versions(origins).cache()
    write_silver(session, settings, versions)

    events = versions.select("event_id").distinct().count()
    moving = (versions.filter("NOT is_first")
              .filter(F.abs(F.col("magnitude_step")) >= 0.05)
              .select("event_id").distinct().count())

    print("-" * 70)
    print(f"  documents Bronze      : {documents.count()}")
    print(f"  versions extraites    : {versions.count()}")
    print(f"  seismes distincts     : {events}")
    print(f"  seismes dont la magnitude bouge : {moving}"
          f"  ({100.0 * moving / events:.1f} %)" if events else "")
    print("-" * 70)

    per_event = versions.groupBy("cohort", "event_id").agg(
        F.first("version_count").alias("version_count"))
    per_event.groupBy("cohort").agg(
        F.count("*").alias("seismes"),
        F.sum("version_count").alias("versions"),
        F.round(F.avg("version_count"), 2).alias("versions_par_seisme"),
        F.max("version_count").alias("versions_max"),
    ).show(truncate=False)

    session.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
