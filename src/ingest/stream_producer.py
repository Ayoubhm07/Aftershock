from __future__ import annotations

import json
import signal
import sys
import time
from datetime import datetime, timezone

from confluent_kafka import KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from src.common.config import LakeSettings, optional
from src.common.usgs import LIVE_HOUR, UsgsUnavailable, fetch_live

FLUSH_TIMEOUT = 15


class GracefulExit:
    def __init__(self) -> None:
        self.requested = False
        signal.signal(signal.SIGINT, self._handle)
        signal.signal(signal.SIGTERM, self._handle)

    def _handle(self, *_) -> None:
        self.requested = True


def build_producer(bootstrap: str) -> Producer:
    return Producer({
        "bootstrap.servers": bootstrap,
        "client.id": "usgs-live-producer",
        "acks": "all",
        "enable.idempotence": True,
        "compression.type": "snappy",
        "linger.ms": 50,
        "retries": 10,
    })


def ensure_topic(bootstrap: str, topic: str, partitions: int = 3,
                 attempts: int = 60) -> None:
    admin = AdminClient({"bootstrap.servers": bootstrap})

    for _ in range(attempts):
        try:
            metadata = admin.list_topics(timeout=5)
        except Exception:
            time.sleep(3)
            continue
        if topic in metadata.topics and metadata.topics[topic].error is None:
            return
        if metadata.brokers:
            break
        time.sleep(3)

    request = NewTopic(topic, num_partitions=partitions, replication_factor=1)
    for name, future in admin.create_topics([request]).items():
        try:
            future.result(timeout=30)
            print(f"topic {name} cree")
        except Exception as error:
            if "already exists" not in str(error).lower():
                raise KafkaException(f"creation de {name} impossible : {error}")


def envelope(feature: dict, fetched_at: str) -> dict:
    return {
        "fetched_at": fetched_at,
        "usgs_id": feature.get("id"),
        "raw": feature,
    }


def publish_snapshot(producer: Producer, topic: str, payload: bytes) -> int:
    document = json.loads(payload)
    fetched_at = datetime.now(timezone.utc).isoformat()
    published = 0

    for feature in document.get("features", []):
        producer.produce(
            topic=topic,
            key=(feature.get("id") or "").encode("utf-8"),
            value=json.dumps(envelope(feature, fetched_at),
                             ensure_ascii=False).encode("utf-8"),
        )
        published += 1

    producer.poll(0)
    producer.flush(FLUSH_TIMEOUT)
    return published


def main() -> int:
    settings = LakeSettings.from_environment()
    interval = float(optional("POLL_INTERVAL_SECONDS", "60"))
    producer = build_producer(settings.kafka_bootstrap)

    print(f"USGS {LIVE_HOUR}")
    print(f"topic {settings.topic_quakes}, cadence {interval:.0f}s")

    try:
        ensure_topic(settings.kafka_bootstrap, settings.topic_quakes)
    except KafkaException as error:
        print(error, file=sys.stderr)
        return 1

    exit_signal = GracefulExit()
    cycles = 0

    while not exit_signal.requested:
        try:
            published = publish_snapshot(producer, settings.topic_quakes, fetch_live())
        except UsgsUnavailable as error:
            print(f"  {error}", file=sys.stderr)
        except (KafkaException, json.JSONDecodeError) as error:
            print(f"  publication impossible : {error}", file=sys.stderr)
        else:
            cycles += 1
            print(f"cycle {cycles:>5} : {published} evenement(s) publie(s)")

        for _ in range(int(interval)):
            if exit_signal.requested:
                break
            time.sleep(1)

    producer.flush(FLUSH_TIMEOUT)
    print(f"arret apres {cycles} cycle(s)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
