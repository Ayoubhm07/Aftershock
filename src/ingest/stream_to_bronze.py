from __future__ import annotations

import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.config import (BRONZE_ROOT, SOURCE_LIVE, ConfigurationError,
                               LakeSettings, optional)
from src.common.spark import build_session

APPLICATION_NAME = "bronze-stream"
TRIGGER_INTERVAL = "30 seconds"
STALL_TOLERANCE_SECONDS = 300


def read_kafka(session: SparkSession, settings: LakeSettings) -> DataFrame:
    return (session.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", settings.kafka_bootstrap)
            .option("subscribe", settings.topic_quakes)
            .option("startingOffsets", optional("STARTING_OFFSETS", "earliest"))
            .option("failOnDataLoss", "false")
            .load())


def to_bronze_rows(stream: DataFrame) -> DataFrame:
    return (stream
            .select(
                F.col("value").cast("string").alias("value"),
                F.col("timestamp").alias("kafka_timestamp"),
                F.col("partition").alias("kafka_partition"),
                F.col("offset").alias("kafka_offset"))
            .withColumn("ingest_date", F.date_format("kafka_timestamp", "yyyy-MM-dd"))
            .select("value", "ingest_date"))


def serve_progress(query) -> None:
    import http.server
    import threading
    import time

    class Probe(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            healthy = query.isActive
            if healthy and query.lastProgress is not None:
                stalled = time.time() - Probe.last_batch[0] > STALL_TOLERANCE_SECONDS
                healthy = not stalled
            self.send_response(200 if healthy else 503)
            self.end_headers()
            self.wfile.write(b"ok" if healthy else b"stalled")

        def log_message(self, *_):
            return

    Probe.last_batch = [time.time()]

    def watch():
        while True:
            if query.lastProgress is not None:
                Probe.last_batch[0] = time.time()
            time.sleep(15)

    port = int(optional("PROGRESS_PORT", "8090"))
    server = http.server.HTTPServer(("0.0.0.0", port), Probe)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    threading.Thread(target=watch, daemon=True).start()
    print(f"sonde de progression sur le port {port}")


def main() -> int:
    try:
        settings = LakeSettings.from_environment()
        session = build_session(APPLICATION_NAME)
    except ConfigurationError as error:
        print(error, file=sys.stderr)
        return 2

    target = f"{settings.hdfs_uri}{BRONZE_ROOT}/source={SOURCE_LIVE}"
    checkpoint = f"{optional('CHECKPOINT_ROOT', 'hdfs://namenode:8020/checkpoints')}/{APPLICATION_NAME}"

    query = (to_bronze_rows(read_kafka(session, settings)).writeStream
             .format("text")
             .option("path", target)
             .option("checkpointLocation", checkpoint)
             .partitionBy("ingest_date")
             .outputMode("append")
             .trigger(processingTime=TRIGGER_INTERVAL)
             .queryName(APPLICATION_NAME)
             .start())

    print(f"{settings.topic_quakes} -> {target}")
    print(f"checkpoint {checkpoint}")

    serve_progress(query)
    query.awaitTermination()
    return 0


if __name__ == "__main__":
    sys.exit(main())
