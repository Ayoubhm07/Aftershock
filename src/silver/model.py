from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (ArrayType, DoubleType, LongType, StringType,
                               StructField, StructType)

from src.silver.magnitude import (FAMILY_UNKNOWN, REFERENCE_FAMILY, SCALES,
                                  UNKNOWN_SATURATION)

ORIGIN_LIVE = "live"
ORIGIN_CATALOG = "catalog"

STATUS_AUTOMATIC = "automatic"
STATUS_REVIEWED = "reviewed"
STATUS_DELETED = "deleted"

FEATURE_PROPERTIES = StructType([
    StructField("mag", DoubleType()),
    StructField("place", StringType()),
    StructField("time", LongType()),
    StructField("updated", LongType()),
    StructField("tz", LongType()),
    StructField("felt", LongType()),
    StructField("cdi", DoubleType()),
    StructField("mmi", DoubleType()),
    StructField("alert", StringType()),
    StructField("status", StringType()),
    StructField("tsunami", LongType()),
    StructField("sig", LongType()),
    StructField("net", StringType()),
    StructField("code", StringType()),
    StructField("ids", StringType()),
    StructField("sources", StringType()),
    StructField("nst", LongType()),
    StructField("dmin", DoubleType()),
    StructField("rms", DoubleType()),
    StructField("gap", DoubleType()),
    StructField("magType", StringType()),
    StructField("type", StringType()),
])

FEATURE = StructType([
    StructField("type", StringType()),
    StructField("id", StringType()),
    StructField("properties", FEATURE_PROPERTIES),
    StructField("geometry", StructType([
        StructField("type", StringType()),
        StructField("coordinates", ArrayType(DoubleType())),
    ])),
])

LIVE_ENVELOPE = StructType([
    StructField("fetched_at", StringType()),
    StructField("usgs_id", StringType()),
    StructField("raw", FEATURE),
])

CATALOG_DOCUMENT = StructType([
    StructField("type", StringType()),
    StructField("features", ArrayType(FEATURE)),
])


def split_ids(raw: Column) -> Column:
    return F.array_sort(F.array_distinct(F.array_remove(
        F.split(F.coalesce(raw, F.lit("")), ","), "")))


def event_key(ids: Column, fallback: Column) -> Column:
    return F.when(F.size(ids) > 0, F.element_at(ids, 1)).otherwise(fallback)


def magnitude_family(mag_type: Column) -> Column:
    token = F.lower(F.coalesce(mag_type, F.lit("")))
    expression = F.lit(FAMILY_UNKNOWN)
    for scale in reversed(SCALES):
        expression = F.when(token.startswith(scale.prefix),
                            F.lit(scale.family)).otherwise(expression)
    return expression


def saturation_threshold(mag_type: Column) -> Column:
    token = F.lower(F.coalesce(mag_type, F.lit("")))
    expression = F.lit(UNKNOWN_SATURATION)
    for scale in reversed(SCALES):
        expression = F.when(token.startswith(scale.prefix),
                            F.lit(scale.saturates_above)).otherwise(expression)
    return expression


def normalise(features: DataFrame, origin: str) -> DataFrame:
    ids = split_ids(F.col("properties.ids"))

    return features.select(
        event_key(ids, F.col("id")).alias("event_key"),
        F.col("id").alias("usgs_id"),
        ids.alias("all_ids"),
        F.array_remove(F.split(F.coalesce(F.col("properties.sources"),
                                          F.lit("")), ","), "").alias("networks"),
        F.col("properties.net").alias("reporting_network"),
        F.col("properties.status").alias("review_status"),
        F.col("properties.mag").alias("magnitude"),
        F.lower(F.col("properties.magType")).alias("magnitude_type"),
        magnitude_family(F.col("properties.magType")).alias("magnitude_family"),
        saturation_threshold(F.col("properties.magType")).alias("saturation_threshold"),
        (F.col("properties.mag") >= saturation_threshold(F.col("properties.magType")))
            .alias("magnitude_saturated"),
        (magnitude_family(F.col("properties.magType")) == F.lit(REFERENCE_FAMILY))
            .alias("magnitude_is_reference"),
        F.to_timestamp(F.col("properties.time") / 1000).alias("event_time"),
        F.to_timestamp(F.col("properties.updated") / 1000).alias("updated_time"),
        F.col("properties.place").alias("place"),
        F.col("geometry.coordinates").getItem(0).alias("longitude"),
        F.col("geometry.coordinates").getItem(1).alias("latitude"),
        F.col("geometry.coordinates").getItem(2).alias("depth_km"),
        F.col("properties.nst").alias("station_count"),
        F.col("properties.gap").alias("azimuthal_gap"),
        F.col("properties.rms").alias("residual_rms"),
        F.col("properties.dmin").alias("nearest_station_deg"),
        F.col("properties.felt").alias("felt_reports"),
        F.col("properties.cdi").alias("community_intensity"),
        F.col("properties.mmi").alias("instrumental_intensity"),
        F.col("properties.alert").alias("alert_level"),
        F.col("properties.tsunami").alias("tsunami_flag"),
        F.col("properties.sig").alias("significance"),
        F.col("properties.type").alias("event_type"),
        F.lit(origin).alias("origin"),
    ).filter(F.col("event_key").isNotNull() & F.col("event_time").isNotNull())


VERSION_COLUMNS = ("magnitude", "magnitude_type", "review_status", "latitude",
                   "longitude", "depth_km", "station_count", "azimuthal_gap",
                   "residual_rms", "usgs_id")


def version_fingerprint() -> Column:
    return F.sha2(F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("~"))
                                     for c in VERSION_COLUMNS]), 256)
