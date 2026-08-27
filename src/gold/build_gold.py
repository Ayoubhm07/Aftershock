from __future__ import annotations

import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.common.config import (GOLD_ROOT, SILVER_ROOT, ConfigurationError,
                               LakeSettings)
from src.common.spark import build_session

APPLICATION_NAME = "gold-tables"
EVENTS_TABLE = f"{SILVER_ROOT}/events"

AUTOMATIC_PREFIX = "usauto"
ALERT_THRESHOLDS = [3.0, 3.5, 4.0, 4.5, 5.0, 5.5, 6.0, 6.5, 7.0]


def read_silver(session: SparkSession, settings: LakeSettings) -> DataFrame:
    return session.read.format("delta").load(f"{settings.hdfs_uri}{EVENTS_TABLE}")


def build_revision_history(events: DataFrame) -> DataFrame:
    timeline = Window.partitionBy("event_key").orderBy("version")
    bounded = timeline.rowsBetween(Window.unboundedPreceding,
                                   Window.unboundedFollowing)

    return (events
            .withColumn("versions", F.count("*").over(bounded))
            .filter(F.col("versions") > 1)
            .withColumn("first_magnitude", F.first("magnitude").over(bounded))
            .withColumn("final_magnitude", F.last("magnitude").over(bounded))
            .withColumn("first_status", F.first("review_status").over(bounded))
            .withColumn("final_status", F.last("review_status").over(bounded))
            .withColumn("first_seen", F.min("valid_from").over(bounded))
            .withColumn("last_seen", F.max("valid_from").over(bounded))
            .filter(F.col("is_current"))
            .select(
                "event_key", "place", "event_time", "versions",
                "first_magnitude", "final_magnitude",
                F.round(F.col("final_magnitude") - F.col("first_magnitude"), 2)
                    .alias("magnitude_shift"),
                F.abs(F.round(F.col("final_magnitude") - F.col("first_magnitude"), 2))
                    .alias("magnitude_shift_abs"),
                "first_status", "final_status",
                "magnitude_type", "magnitude_family",
                "station_count", "azimuthal_gap", "residual_rms",
                F.round((F.col("last_seen").cast("long")
                         - F.col("first_seen").cast("long")) / 60.0, 1)
                    .alias("minutes_to_final"))
            .orderBy(F.desc("magnitude_shift_abs")))


def build_identity_history(events: DataFrame) -> DataFrame:
    current = events.filter("is_current")

    return (current
            .withColumn("identifier_count", F.size("all_ids"))
            .withColumn(
                "automatic_origin",
                F.exists("all_ids", lambda i: i.startswith(AUTOMATIC_PREFIX)))
            .withColumn("identity_changed",
                        F.col("automatic_origin")
                        & ~F.col("usgs_id").startswith(AUTOMATIC_PREFIX))
            .select("event_key", "usgs_id", "all_ids", "networks",
                    "reporting_network", "identifier_count", "automatic_origin",
                    "identity_changed", "review_status", "magnitude",
                    "magnitude_type", "event_time", "place")
            .orderBy(F.desc("identifier_count")))


def build_alert_reliability(revisions: DataFrame, session: SparkSession) -> DataFrame:
    pairs = revisions.filter(
        F.col("first_magnitude").isNotNull() & F.col("final_magnitude").isNotNull())

    thresholds = session.createDataFrame(
        [(t,) for t in ALERT_THRESHOLDS], "threshold double")

    joined = pairs.crossJoin(F.broadcast(thresholds))

    raised = F.col("first_magnitude") >= F.col("threshold")
    confirmed = F.col("final_magnitude") >= F.col("threshold")

    return (joined
            .groupBy("threshold")
            .agg(
                F.count("*").alias("events_evaluated"),
                F.sum(raised.cast("int")).alias("alerts_raised"),
                F.sum((raised & confirmed).cast("int")).alias("alerts_confirmed"),
                F.sum((raised & ~confirmed).cast("int")).alias("false_alerts"),
                F.sum((~raised & confirmed).cast("int")).alias("missed_events"))
            .withColumn(
                "false_alert_rate",
                F.when(F.col("alerts_raised") > 0,
                       F.round(F.col("false_alerts") / F.col("alerts_raised"), 4)))
            .withColumn(
                "miss_rate",
                F.when(F.col("events_evaluated") > 0,
                       F.round(F.col("missed_events") / F.col("events_evaluated"), 4)))
            .orderBy("threshold"))


def build_magnitude_scales(events: DataFrame) -> DataFrame:
    return (events.filter("is_current")
            .groupBy("magnitude_family", "magnitude_type")
            .agg(F.count("*").alias("events"),
                 F.round(F.avg("magnitude"), 3).alias("magnitude_avg"),
                 F.round(F.min("magnitude"), 2).alias("magnitude_min"),
                 F.round(F.max("magnitude"), 2).alias("magnitude_max"),
                 F.first("saturation_threshold").alias("saturation_threshold"),
                 F.sum(F.col("magnitude_saturated").cast("int")).alias("saturated"))
            .orderBy(F.desc("events")))


def build_review_lag(events: DataFrame) -> DataFrame:
    return (events.filter("is_current")
            .withColumn("review_lag_minutes",
                        F.round((F.col("updated_time").cast("long")
                                 - F.col("event_time").cast("long")) / 60.0, 1))
            .filter("review_lag_minutes >= 0")
            .groupBy("review_status", "magnitude_family")
            .agg(F.count("*").alias("events"),
                 F.round(F.avg("review_lag_minutes"), 1).alias("lag_avg_minutes"),
                 F.round(F.expr("percentile_approx(review_lag_minutes, 0.5)"), 1)
                    .alias("lag_median_minutes"),
                 F.round(F.expr("percentile_approx(review_lag_minutes, 0.9)"), 1)
                    .alias("lag_p90_minutes"))
            .orderBy(F.desc("events")))


def write(frame: DataFrame, settings: LakeSettings, name: str) -> str:
    target = f"{settings.hdfs_uri}{GOLD_ROOT}/{name}"
    frame.coalesce(1).write.mode("overwrite").parquet(target)
    return target


def main() -> int:
    try:
        settings = LakeSettings.from_environment()
        session = build_session(APPLICATION_NAME, with_delta=True)
    except ConfigurationError as error:
        print(error, file=sys.stderr)
        return 2

    events = read_silver(session, settings).cache()
    revisions = build_revision_history(events).cache()

    tables = {
        "revision_history": revisions,
        "identity_history": build_identity_history(events),
        "alert_reliability": build_alert_reliability(revisions, session),
        "magnitude_scales": build_magnitude_scales(events),
        "review_lag": build_review_lag(events),
    }

    print("-" * 70)
    for name, frame in tables.items():
        target = write(frame, settings, name)
        print(f"  {name:<20} {frame.count():>7} lignes  ->  {target}")
    print("-" * 70)

    identity = tables["identity_history"]
    traced = identity.filter("automatic_origin").count()
    changed = identity.filter("identity_changed").count()

    print(f"  seismes portant la trace d'une solution automatique : {traced}")
    print(f"  dont l'identifiant retenu a CHANGE depuis           : {changed}")
    print(f"  seismes revises observes par notre flux             : {revisions.count()}")
    print("-" * 70)

    tables["magnitude_scales"].show(10, truncate=False)
    tables["review_lag"].show(10, truncate=False)

    session.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
