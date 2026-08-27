from __future__ import annotations

import os
from dataclasses import dataclass


class ConfigurationError(RuntimeError):
    pass


def required(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise ConfigurationError(f"Variable d'environnement obligatoire absente : {name}")
    return value


def optional(name: str, fallback: str) -> str:
    return os.environ.get(name, "").strip() or fallback


BRONZE_ROOT = "/lake/bronze"
SILVER_ROOT = "/lake/silver"
GOLD_ROOT = "/lake/gold"

SOURCE_LIVE = "usgs_live"
SOURCE_CATALOG = "usgs_catalog"
SOURCE_VERSIONS = "usgs_versions"

VERSIONS_BATCH_SIZE = 100

INGESTED_MARKER = "_SUCCESS"


@dataclass(frozen=True)
class LakeSettings:
    hdfs_uri: str
    webhdfs_url: str
    kafka_bootstrap: str
    topic_quakes: str

    @staticmethod
    def from_environment() -> "LakeSettings":
        return LakeSettings(
            hdfs_uri=optional("HDFS_URI", "hdfs://namenode:8020"),
            webhdfs_url=optional("WEBHDFS_URL", "http://namenode:9870"),
            kafka_bootstrap=optional("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            topic_quakes=optional("TOPIC_QUAKES", "quakes_live"),
        )

    def bronze_live(self, day: str) -> str:
        return f"{BRONZE_ROOT}/source={SOURCE_LIVE}/ingest_date={day}"

    def bronze_catalog(self, year: int, month: int) -> str:
        return (f"{BRONZE_ROOT}/source={SOURCE_CATALOG}"
                f"/year={year:04d}/month={month:02d}")

    def bronze_versions(self, batch: int) -> str:
        return f"{BRONZE_ROOT}/source={SOURCE_VERSIONS}/batch={batch:04d}"
