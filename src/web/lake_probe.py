from __future__ import annotations

import json
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field

from src.common.config import (BRONZE_ROOT, GOLD_ROOT, SILVER_ROOT,
                               SOURCE_CATALOG, SOURCE_LIVE, SOURCE_VERSIONS)
from src.common.hdfs import HdfsError, WebHdfs

REFRESH_SECONDS = 20
PROBE_TIMEOUT = 4


def summary(url: str) -> dict | None:
    try:
        with urllib.request.urlopen(url, timeout=PROBE_TIMEOUT) as response:
            return json.loads(response.read())
    except (urllib.error.URLError, ValueError, OSError):
        return None


@dataclass
class Probe:
    """Etat de la pile, rafraichi en arriere-plan. Sonder a chaque requete
    ferait payer au navigateur la latence de HDFS ; le cache borne garde le
    tableau de bord reactif meme quand un service repond lentement."""

    webhdfs: str
    spark_ui: str
    state: dict = field(default_factory=dict)
    refreshed_at: float = 0.0
    lock: threading.Lock = field(default_factory=threading.Lock)

    def start(self) -> None:
        threading.Thread(target=self._loop, daemon=True).start()

    def _loop(self) -> None:
        while True:
            try:
                fresh = self._collect()
            except Exception as error:                      # noqa: BLE001
                fresh = {"erreur": f"{type(error).__name__}: {error}"}
            with self.lock:
                self.state = fresh
                self.refreshed_at = time.time()
            time.sleep(REFRESH_SECONDS)

    def _collect(self) -> dict:
        fs = WebHdfs(self.webhdfs)

        layers = {}
        for name, path in (
            ("bronze_flux", f"{BRONZE_ROOT}/source={SOURCE_LIVE}"),
            ("bronze_catalogue", f"{BRONZE_ROOT}/source={SOURCE_CATALOG}"),
            ("bronze_versions", f"{BRONZE_ROOT}/source={SOURCE_VERSIONS}"),
            ("silver", SILVER_ROOT),
            ("gold", GOLD_ROOT),
        ):
            try:
                layers[name] = fs.content_summary(path)
            except HdfsError:
                layers[name] = {"fichiers": 0, "repertoires": 0, "octets": 0}

        cluster = summary(f"{self.spark_ui}/json/") or {}
        applications = [
            {"nom": app.get("name"), "coeurs": app.get("cores"),
             "memoire_mo": app.get("memoryperexecutor"),
             "secondes": (app.get("duration") or 0) // 1000}
            for app in cluster.get("activeapps", [])
        ]

        return {
            "couches": layers,
            "spark": {
                "coeurs": cluster.get("cores"),
                "coeurs_utilises": cluster.get("coresused"),
                "memoire_mo": cluster.get("memory"),
                "memoire_utilisee_mo": cluster.get("memoryused"),
                "applications": applications,
            } if cluster else None,
        }

    def snapshot(self) -> dict:
        with self.lock:
            payload = dict(self.state)
            payload["rafraichi_il_y_a"] = (
                round(time.time() - self.refreshed_at)
                if self.refreshed_at else None)
            return payload
