from __future__ import annotations

import json
import sys
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

from src.common.config import (GOLD_ROOT, SILVER_ROOT, ConfigurationError,
                               LakeSettings, optional)
from src.common.hdfs import HdfsError, WebHdfs
from src.common.spark import build_session

APPLICATION_NAME = "gold-export-site"

VERSIONS_TABLE = f"{SILVER_ROOT}/event_versions"
DATASET_TABLE = f"{GOLD_ROOT}/magnitude_revision"
CURVE_TABLE = f"{GOLD_ROOT}/alert_curve"
SCALES_TABLE = f"{GOLD_ROOT}/magnitude_scales"
IDENTITY_TABLE = f"{GOLD_ROOT}/identity_history"
REPORT_PATH = f"{GOLD_ROOT}/model_report/report.json"
BUNDLE_PATH = f"{GOLD_ROOT}/site_bundle.json"

FEATURED_EVENTS = 160
TIMELINE_COLUMNS = ("version_rank", "published_at", "contributor", "magnitude",
                    "magnitude_type", "review_status", "station_count",
                    "azimuthal_gap", "minutes_since_quake", "depth_km")


def rows(frame: DataFrame, limit: int | None = None) -> list[dict]:
    collected = frame.limit(limit).collect() if limit else frame.collect()
    return [{key: (value.isoformat() if hasattr(value, "isoformat") else value)
             for key, value in row.asDict().items()}
            for row in collected]


def read_optional(session: SparkSession, path: str, fmt: str) -> DataFrame | None:
    try:
        return session.read.format(fmt).load(path)
    except AnalysisException:
        return None


def featured_keys(dataset: DataFrame) -> list[str]:
    ranked = dataset.orderBy(F.desc("shift_abs")).limit(FEATURED_EVENTS)
    return [row["event_id"] for row in ranked.select("event_id").collect()]


def timelines(versions: DataFrame, keys: list[str]) -> dict[str, list[dict]]:
    selected = (versions.filter(F.col("event_id").isin(keys))
                .orderBy("event_id", "version_rank")
                .select("event_id", *TIMELINE_COLUMNS))
    grouped: dict[str, list[dict]] = {}
    for row in rows(selected):
        grouped.setdefault(row.pop("event_id"), []).append(row)
    return grouped


def overview(dataset: DataFrame, versions: DataFrame,
             identity: DataFrame | None) -> dict:
    totals = dataset.agg(
        F.count("*").alias("apparies"),
        F.sum(F.col("moved").cast("int")).alias("deplaces"),
        F.round(F.avg("shift"), 3).alias("ecart_moyen"),
        F.round(F.max("shift_abs"), 2).alias("ecart_max"),
        F.round(F.expr("percentile_approx(shift_abs, 0.5)"), 2).alias("ecart_median"),
        F.round(F.expr("percentile_approx(first_minutes_since_quake, 0.5)"), 1)
            .alias("delai_premiere_publication"),
        F.round(F.expr("percentile_approx(review_delay_minutes, 0.5)"), 1)
            .alias("delai_stabilisation"),
        F.sum(F.col("scale_changed").cast("int")).alias("changements_echelle"),
    ).first().asDict()

    totals["versions_totales"] = versions.count()
    totals["seismes_suivis"] = versions.select("event_id").distinct().count()
    if identity is not None:
        totals["catalogue"] = identity.count()
        totals["identite_changee"] = identity.filter("identity_changed").count()
    return totals


def cohort_comparison(dataset: DataFrame) -> list[dict]:
    return rows(dataset.groupBy("cohort").agg(
        F.count("*").alias("seismes"),
        F.round(F.avg(F.col("moved").cast("int")) * 100, 1).alias("pct_deplace"),
        F.round(F.avg("shift"), 3).alias("ecart_moyen"),
        F.round(F.avg(F.col("scale_changed").cast("int")) * 100, 1)
            .alias("pct_changement_echelle"),
    ).orderBy("cohort"))


def family_breakdown(dataset: DataFrame) -> list[dict]:
    return rows(dataset.groupBy("first_family", "final_family").agg(
        F.count("*").alias("seismes"),
        F.round(F.avg("shift"), 3).alias("ecart_moyen"),
    ).orderBy(F.desc("seismes")).limit(20))


def main() -> int:
    try:
        settings = LakeSettings.from_environment()
        session = build_session(APPLICATION_NAME, with_delta=True)
    except ConfigurationError as error:
        print(error, file=sys.stderr)
        return 2

    base = settings.hdfs_uri
    dataset = read_optional(session, f"{base}{DATASET_TABLE}", "parquet")
    versions = read_optional(session, f"{base}{VERSIONS_TABLE}", "delta")

    if dataset is None or versions is None:
        print("jeu de revisions ou table de versions absent", file=sys.stderr)
        session.stop()
        return 1

    dataset = dataset.cache()
    curve = read_optional(session, f"{base}{CURVE_TABLE}", "parquet")
    scales = read_optional(session, f"{base}{SCALES_TABLE}", "parquet")
    identity = read_optional(session, f"{base}{IDENTITY_TABLE}", "parquet")

    fs = WebHdfs(settings.webhdfs_url)
    try:
        model_report = json.loads(fs.read(REPORT_PATH))
    except (HdfsError, ValueError):
        model_report = None

    keys = featured_keys(dataset)
    featured = (dataset.filter(F.col("event_id").isin(keys))
                .select("event_id", "cohort", "place", "event_time",
                        "first_magnitude", "first_magnitude_type",
                        "first_contributor", "first_station_count",
                        "first_azimuthal_gap", "first_minutes_since_quake",
                        "final_magnitude", "final_magnitude_type",
                        "shift", "shift_abs", "direction", "scale_changed",
                        "version_count", "review_delay_minutes")
                .orderBy(F.desc("shift_abs")))

    bundle = {
        "genere_le": datetime.now(timezone.utc).isoformat(),
        "overview": overview(dataset, versions, identity),
        "cohortes": cohort_comparison(dataset),
        "familles": family_breakdown(dataset),
        "courbe_alerte": rows(curve.orderBy("threshold")) if curve else [],
        "echelles": rows(scales) if scales else [],
        "seismes": rows(featured),
        "chronologies": timelines(versions, keys),
        "modele": model_report,
    }

    payload = json.dumps(bundle, ensure_ascii=False, separators=(",", ":"))
    fs.write(BUNDLE_PATH, payload.encode("utf-8"))

    print("-" * 70)
    print(f"  bundle ecrit dans {BUNDLE_PATH}")
    print(f"  {len(payload) / 1024:.1f} Ko")
    print(f"  {len(bundle['seismes'])} seismes detailles, "
          f"{sum(len(v) for v in bundle['chronologies'].values())} versions")
    print(f"  courbe : {len(bundle['courbe_alerte'])} seuils")
    print(f"  modele : {'present' if model_report else 'absent'}")
    print("-" * 70)

    session.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
