from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F
from pyspark.sql.types import (ArrayType, DoubleType, LongType, MapType,
                               StringType, StructField, StructType)

ORIGIN_PRODUCT = StructType([
    StructField("source", StringType()),
    StructField("code", StringType()),
    StructField("status", StringType()),
    StructField("updateTime", LongType()),
    StructField("preferredWeight", LongType()),
    StructField("properties", MapType(StringType(), StringType())),
])

VERSIONS_DOCUMENT = StructType([
    StructField("event_id", StringType()),
    StructField("event_time", LongType()),
    StructField("current_magnitude", DoubleType()),
    StructField("current_magnitude_type", StringType()),
    StructField("place", StringType()),
    StructField("cohort", StringType()),
    StructField("products", StructType([
        StructField("origin", ArrayType(ORIGIN_PRODUCT)),
    ])),
])

NUMERIC_FIELDS = {
    "magnitude": "magnitude",
    "station_count": "num-stations-used",
    "magnitude_station_count": "magnitude-num-stations-used",
    "azimuthal_gap": "azimuthal-gap",
    "minimum_distance": "minimum-distance",
    "phase_count": "num-phases-used",
    "standard_error": "standard-error",
    "horizontal_error": "horizontal-error",
    "vertical_error": "vertical-error",
    "magnitude_error": "magnitude-error",
    "depth_km": "depth",
    "latitude": "latitude",
    "longitude": "longitude",
}

TEXT_FIELDS = {
    "magnitude_type": "magnitude-type",
    "review_status": "review-status",
    "depth_type": "depth-type",
    "location_method": "location-method-algorithm",
    "event_type": "event-type",
}


def property_at(field: str) -> Column:
    return F.col("origin.properties").getItem(field)


def numeric(field: str) -> Column:
    return property_at(field).cast("double")


def text(field: str) -> Column:
    return F.lower(F.trim(property_at(field)))


def origin_columns() -> list[Column]:
    numbers = [numeric(source).alias(name)
               for name, source in NUMERIC_FIELDS.items()]
    words = [text(source).alias(name) for name, source in TEXT_FIELDS.items()]
    return numbers + words
