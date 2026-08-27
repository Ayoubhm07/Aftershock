from __future__ import annotations

import json
import threading
import time
from collections import deque
from dataclasses import dataclass, field

from confluent_kafka import Consumer, KafkaError, TopicPartition

RING_SIZE = 400
RATE_WINDOW_SECONDS = 300
BACKFILL_MINUTES = 10
POLL_TIMEOUT = 1.0


@dataclass
class Seen:
    magnitude: float | None
    magnitude_type: str | None
    status: str | None
    updated: int | None


@dataclass
class Tap:
    """Consomme le topic en continu et garde en memoire ce qui vient de
    passer. Le producteur republie tout l'instantane horaire a chaque cycle :
    un identifiant deja vu dont la magnitude a change est une revision
    observee en direct, et c'est le seul endroit de la chaine ou elle est
    visible a la seconde."""

    bootstrap: str
    topic: str
    events: deque = field(default_factory=lambda: deque(maxlen=RING_SIZE))
    revisions: deque = field(default_factory=lambda: deque(maxlen=60))
    stamps: deque = field(default_factory=lambda: deque(maxlen=5000))
    known: dict[str, Seen] = field(default_factory=dict)
    total: int = 0
    first_seen_count: int = 0
    started_at: float = field(default_factory=time.time)
    last_message_at: float | None = None
    error: str | None = None
    lock: threading.Lock = field(default_factory=threading.Lock)

    def consumer(self) -> Consumer:
        return Consumer({
            "bootstrap.servers": self.bootstrap,
            "group.id": f"dashboard-{int(time.time())}",
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
            "session.timeout.ms": 10000,
        })

    def start(self) -> None:
        threading.Thread(target=self._run, daemon=True).start()

    def _run(self) -> None:
        while True:
            try:
                self._consume()
            except Exception as error:                      # noqa: BLE001
                with self.lock:
                    self.error = f"{type(error).__name__}: {error}"
                time.sleep(5)

    def _backfill(self, client, partitions) -> None:
        """Remonter de BACKFILL_MINUTES : sans cela le tableau de bord reste
        vide jusqu'au prochain cycle du producteur, soit jusqu'a une minute
        d'ecran noir devant un jury.

        Fournir un on_assign REMPLACE l'assignation par defaut : il faut
        appeler assign() soi-meme, sinon le consommateur n'obtient aucune
        partition et se tait sans lever d'erreur."""
        since = int((time.time() - BACKFILL_MINUTES * 60) * 1000)
        wanted = [TopicPartition(p.topic, p.partition, since)
                  for p in partitions]
        try:
            resolved = client.offsets_for_times(wanted, timeout=10)
        except Exception:                                   # noqa: BLE001
            client.assign(partitions)
            return

        placed = []
        for found, fallback in zip(resolved, partitions):
            if found.offset is not None and found.offset >= 0:
                placed.append(found)
            else:
                placed.append(fallback)
        client.assign(placed)

    def _consume(self) -> None:
        client = self.consumer()
        client.subscribe([self.topic],
                         on_assign=lambda c, p: self._backfill(c, p))
        with self.lock:
            self.error = None
        try:
            while True:
                message = client.poll(POLL_TIMEOUT)
                if message is None:
                    continue
                if message.error():
                    if message.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise RuntimeError(message.error())
                self._absorb(message)
        finally:
            client.close()

    def _absorb(self, message) -> None:
        try:
            envelope = json.loads(message.value())
        except ValueError:
            return

        raw = envelope.get("raw") or {}
        properties = raw.get("properties") or {}
        geometry = (raw.get("geometry") or {}).get("coordinates") or []
        usgs_id = envelope.get("usgs_id") or raw.get("id")
        if not usgs_id:
            return

        record = {
            "id": usgs_id,
            "place": properties.get("place"),
            "magnitude": properties.get("mag"),
            "magnitude_type": properties.get("magType"),
            "status": properties.get("status"),
            "time": properties.get("time"),
            "updated": properties.get("updated"),
            "felt": properties.get("felt"),
            "tsunami": properties.get("tsunami"),
            "longitude": geometry[0] if len(geometry) > 0 else None,
            "latitude": geometry[1] if len(geometry) > 1 else None,
            "depth": geometry[2] if len(geometry) > 2 else None,
            "partition": message.partition(),
            "offset": message.offset(),
            "received_at": time.time(),
            "produced_at": (message.timestamp()[1] or 0) / 1000.0,
        }

        current = Seen(record["magnitude"], record["magnitude_type"],
                       record["status"], record["updated"])

        with self.lock:
            self.total += 1
            self.last_message_at = record["received_at"]
            self.stamps.append(record["produced_at"] or record["received_at"])

            previous = self.known.get(usgs_id)
            if previous is None:
                self.first_seen_count += 1
                record["kind"] = "nouveau"
            elif (previous.magnitude != current.magnitude
                  or previous.status != current.status):
                record["kind"] = "revise"
                record["previous_magnitude"] = previous.magnitude
                record["previous_status"] = previous.status
                self.revisions.appendleft(dict(record))
            else:
                record["kind"] = "repete"

            self.known[usgs_id] = current
            self.events.appendleft(record)

    def rate_per_minute(self) -> float:
        """Cadence mesuree sur l'horodatage de PRODUCTION. La calculer sur
        l'heure de reception ferait passer le rattrapage initial pour un pic
        de trafic : dix minutes de messages avalees en une seconde."""
        horizon = time.time() - RATE_WINDOW_SECONDS
        recent = sorted(stamp for stamp in self.stamps if stamp >= horizon)
        if len(recent) < 2:
            return 0.0
        span = max(recent[-1] - recent[0], 1.0)
        return round(len(recent) * 60.0 / span, 1)

    def snapshot(self, limit: int = 60) -> dict:
        with self.lock:
            events = list(self.events)[:limit]
            revisions = list(self.revisions)[:12]
            return {
                "connecte": self.error is None,
                "erreur": self.error,
                "messages_total": self.total,
                "seismes_distincts": len(self.known),
                "premieres_apparitions": self.first_seen_count,
                "revisions_captees": len(self.revisions),
                "cadence_par_minute": self.rate_per_minute(),
                "depuis_secondes": round(time.time() - self.started_at),
                "dernier_message_il_y_a":
                    round(time.time() - self.last_message_at)
                    if self.last_message_at else None,
                "evenements": events,
                "revisions": revisions,
            }
