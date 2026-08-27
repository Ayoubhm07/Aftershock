from __future__ import annotations

import sys

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException
from pyspark.sql.window import Window

from src.common.config import (BRONZE_ROOT, SILVER_ROOT, SOURCE_CATALOG,
                               SOURCE_LIVE, ConfigurationError, LakeSettings)
from src.common.spark import build_session
from src.silver.model import (CATALOG_DOCUMENT, LIVE_ENVELOPE, ORIGIN_CATALOG,
                              ORIGIN_LIVE, normalise, version_fingerprint)

APPLICATION_NAME = "silver-events"
EVENTS_TABLE = f"{SILVER_ROOT}/events"


def read_live(session: SparkSession, settings: LakeSettings) -> DataFrame | None:
    path = f"{settings.hdfs_uri}{BRONZE_ROOT}/source={SOURCE_LIVE}/ingest_date=*/*.txt"
    try:
        envelopes = session.read.schema(LIVE_ENVELOPE).json(path)
    except AnalysisException:
        return None

    features = (envelopes
                .filter(F.col("raw.id").isNotNull())
                .select(F.col("raw.id").alias("id"),
                        F.col("raw.properties").alias("properties"),
                        F.col("raw.geometry").alias("geometry")))

    return normalise(features, ORIGIN_LIVE)


def read_catalog(session: SparkSession, settings: LakeSettings) -> DataFrame | None:
    path = (f"{settings.hdfs_uri}{BRONZE_ROOT}/source={SOURCE_CATALOG}"
            f"/year=*/month=*/events.geojson")
    try:
        documents = (session.read
                     .option("multiLine", "true")
                     .schema(CATALOG_DOCUMENT)
                     .json(path))
    except AnalysisException:
        return None

    features = documents.select(F.explode("features").alias("f")).select("f.*")
    return normalise(features, ORIGIN_CATALOG)


def deduplicate_versions(events: DataFrame) -> DataFrame:
    ranked = Window.partitionBy("event_key", "fingerprint").orderBy(
        F.col("updated_time").asc_nulls_last(), F.col("origin"))
    return (events
            .withColumn("fingerprint", version_fingerprint())
            .withColumn("rank", F.row_number().over(ranked))
            .filter(F.col("rank") == 1)
            .drop("rank"))


def resolve_timeline(events: DataFrame) -> DataFrame:
    timeline = Window.partitionBy("event_key").orderBy(
        F.col("updated_time").asc_nulls_last(), F.col("usgs_id"))
    return (events
            .withColumn("version", F.row_number().over(timeline))
            .withColumn("valid_from", F.col("updated_time"))
            .withColumn("valid_to", F.lead("updated_time").over(timeline))
            .withColumn("is_current", F.lead("updated_time").over(timeline).isNull())
            .withColumn("previous_magnitude", F.lag("magnitude").over(timeline))
            .withColumn("magnitude_delta",
                        F.round(F.col("magnitude") - F.lag("magnitude").over(timeline), 2))
            .withColumn("previous_status", F.lag("review_status").over(timeline)))


def write_silver(session: SparkSession, settings: LakeSettings,
                 versions: DataFrame) -> None:
    target = f"{settings.hdfs_uri}{EVENTS_TABLE}"

    if not DeltaTable.isDeltaTable(session, target):
        (versions.write.format("delta")
         .partitionBy("origin")
         .mode("overwrite")
         .save(target))
        return

    (DeltaTable.forPath(session, target).alias("t")
     .merge(versions.alias("s"),
            "t.event_key = s.event_key AND t.fingerprint = s.fingerprint")
     .whenMatchedUpdateAll()
     .whenNotMatchedInsertAll()
     .execute())


def main() -> int:
    try:
        settings = LakeSettings.from_environment()
        session = build_session(APPLICATION_NAME, with_delta=True)
    except ConfigurationError as error:
        print(error, file=sys.stderr)
        return 2

    catalog = read_catalog(session, settings)
    live = read_live(session, settings)

    sources = [frame for frame in (catalog, live) if frame is not None]
    if not sources:
        print("aucune donnee en Bronze", file=sys.stderr)
        session.stop()
        return 1

    combined = sources[0]
    for extra in sources[1:]:
        combined = combined.unionByName(extra, allowMissingColumns=True)

    versions = resolve_timeline(deduplicate_versions(combined)).cache()
    write_silver(session, settings, versions)

    stored = session.read.format("delta").load(f"{settings.hdfs_uri}{EVENTS_TABLE}")
    distinct_events = stored.select("event_key").distinct().count()
    revised = (stored.groupBy("event_key").count()
               .filter(F.col("count") > 1).count())

    print("-" * 66)
    print(f"  catalogue        : {catalog.count() if catalog is not None else 0} enregistrements")
    print(f"  flux temps reel  : {live.count() if live is not None else 0} enregistrements")
    print(f"  versions Silver  : {stored.count()}")
    print(f"  seismes distincts: {distinct_events}")
    print(f"  seismes revises  : {revised}")
    print("-" * 66)

    stored.groupBy("magnitude_family").count().orderBy(F.desc("count")).show(truncate=False)
    stored.groupBy("review_status").count().orderBy(F.desc("count")).show(truncate=False)

    print("  magnitudes saturees, donc non comparables hors famille :")
    stored.filter("magnitude_saturated").groupBy("magnitude_type").count() \
          .orderBy(F.desc("count")).show(10, truncate=False)

    session.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
