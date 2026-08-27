from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request

from pyspark.sql import SparkSession

from src.common.config import ConfigurationError, optional, required

WORKER_WAIT_SECONDS = 180


def resolve_master() -> str:
    master = required("SPARK_MASTER_URL")
    if master.startswith("local"):
        raise ConfigurationError(
            f"SPARK_MASTER_URL={master!r} est une session locale ; "
            "un master standalone est attendu.")
    if not master.startswith("spark://"):
        raise ConfigurationError(f"SPARK_MASTER_URL={master!r} invalide.")
    return master


def wait_for_workers(master_ui: str, timeout: int = WORKER_WAIT_SECONDS) -> int:
    deadline = time.time() + timeout
    last = "aucune reponse du master"
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(f"{master_ui}/json/", timeout=5) as response:
                status = json.loads(response.read().decode())
            alive = int(status.get("aliveworkers", 0))
            if alive:
                return alive
            last = "master vivant, aucun worker"
        except (urllib.error.URLError, ValueError, OSError) as error:
            last = f"{type(error).__name__}: {error}"
        time.sleep(3)
    raise ConfigurationError(f"aucun worker Spark apres {timeout}s ({last})")


def build_session(application_name: str, with_delta: bool = False) -> SparkSession:
    master = resolve_master()
    workers = wait_for_workers(optional("SPARK_MASTER_UI", "http://spark-master:8080"))

    builder = (
        SparkSession.builder.appName(application_name)
        .master(master)
        .config("spark.hadoop.fs.defaultFS", optional("HDFS_URI", "hdfs://namenode:8020"))
        .config("spark.hadoop.dfs.replication", "1")
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
        .config("spark.sql.shuffle.partitions", optional("SHUFFLE_PARTITIONS", "4"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.executor.memory", optional("EXECUTOR_MEMORY", "1g"))
        .config("spark.cores.max", optional("CORES_MAX", "2"))
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
    )

    if with_delta:
        builder = (builder
                   .config("spark.sql.extensions",
                           "io.delta.sql.DeltaSparkSessionExtension")
                   .config("spark.sql.catalog.spark_catalog",
                           "org.apache.spark.sql.delta.catalog.DeltaCatalog"))

    driver_host = os.environ.get("SPARK_DRIVER_HOST", "").strip()
    if driver_host:
        builder = builder.config("spark.driver.host", driver_host)

    session = builder.getOrCreate()
    session.sparkContext.setLogLevel(optional("SPARK_LOG_LEVEL", "WARN"))
    print(f"{application_name} -> {session.sparkContext.master} ({workers} worker)")
    return session
