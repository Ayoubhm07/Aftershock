from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

NUMERIC_FEATURES = (
    "first_magnitude",
    "first_station_count",
    "first_magnitude_station_count",
    "first_azimuthal_gap",
    "first_minimum_distance",
    "first_phase_count",
    "first_standard_error",
    "first_horizontal_error",
    "first_magnitude_error",
    "first_depth_km",
    "first_minutes_since_quake",
    "first_saturation",
    "first_latitude",
    "first_longitude",
)

CATEGORICAL_FEATURES = (
    "first_magnitude_type",
    "first_review_status",
    "first_contributor",
    "first_family",
)

TARGET = "shift"

FORBIDDEN = (
    "final_magnitude", "final_magnitude_type", "final_review_status",
    "final_contributor", "final_station_count", "final_azimuthal_gap",
    "final_minutes_since_quake", "shift", "shift_abs", "moved", "direction",
    "final_family", "scale_changed", "station_growth",
    "review_delay_minutes", "version_count",
)


class LeakageDetected(RuntimeError):
    pass


def assert_no_leakage() -> None:
    used = set(NUMERIC_FEATURES) | set(CATEGORICAL_FEATURES)
    leaked = used & set(FORBIDDEN)
    if leaked:
        raise LeakageDetected(
            f"variables connues seulement apres la revision : {sorted(leaked)}")


def missing_indicators(dataset: DataFrame) -> DataFrame:
    frame = dataset
    for name in NUMERIC_FEATURES:
        frame = frame.withColumn(f"{name}_absent",
                                 F.col(name).isNull().cast("double"))
    return frame


def indicator_names() -> tuple[str, ...]:
    return tuple(f"{name}_absent" for name in NUMERIC_FEATURES)


def split_by_time(dataset: DataFrame, train_ratio: float = 0.7
                  ) -> tuple[DataFrame, DataFrame, str]:
    boundary = dataset.selectExpr(
        f"percentile_approx(unix_timestamp(event_time), {train_ratio}) AS cut"
    ).first()["cut"]

    cut_column = F.unix_timestamp("event_time")
    train = dataset.filter(cut_column <= boundary)
    test = dataset.filter(cut_column > boundary)

    frontier = (dataset.filter(cut_column <= boundary)
                .agg(F.max("event_time")).first()[0])
    return train, test, str(frontier)
