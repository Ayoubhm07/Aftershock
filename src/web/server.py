from __future__ import annotations

import json
import pathlib
import sys
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from src.common.config import GOLD_ROOT, LakeSettings, optional
from src.common.hdfs import HdfsError, WebHdfs
from src.web.kafka_tap import Tap
from src.web.lake_probe import Probe

BUNDLE_PATH = f"{GOLD_ROOT}/site_bundle.json"
MODEL_PATH = f"{GOLD_ROOT}/model_export/trees.json"
PAGE = pathlib.Path("/opt/app/site/aftershock.html")

STREAM_INTERVAL = 2.0
BUNDLE_TTL = 60.0


class Cache:
    def __init__(self, fs: WebHdfs) -> None:
        self.fs = fs
        self.entries: dict[str, tuple[float, bytes]] = {}

    def fetch(self, path: str) -> bytes | None:
        now = time.time()
        cached = self.entries.get(path)
        if cached and now - cached[0] < BUNDLE_TTL:
            return cached[1]
        try:
            payload = self.fs.read(path)
        except HdfsError:
            return cached[1] if cached else None
        self.entries[path] = (now, payload)
        return payload


def handler_factory(tap: Tap, probe: Probe, cache: Cache):

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *_):
            return

        def _headers(self, status: int, content_type: str,
                     length: int | None = None, stream: bool = False) -> None:
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Cache-Control", "no-store")
            if stream:
                self.send_header("Connection", "keep-alive")
            elif length is not None:
                self.send_header("Content-Length", str(length))
            self.end_headers()

        def _json(self, payload: dict, status: int = 200) -> None:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self._headers(status, "application/json; charset=utf-8", len(body))
            self.wfile.write(body)

        def _raw(self, payload: bytes | None, content_type: str) -> None:
            if payload is None:
                self._json({"erreur": "ressource absente du lac"}, 404)
                return
            self._headers(200, content_type, len(payload))
            self.wfile.write(payload)

        def _page(self) -> None:
            if not PAGE.exists():
                self._json({"erreur": "page non construite, lancer make site"}, 404)
                return
            body = PAGE.read_bytes()
            self._headers(200, "text/html; charset=utf-8", len(body))
            self.wfile.write(body)

        def _stream(self) -> None:
            self._headers(200, "text/event-stream; charset=utf-8", stream=True)
            try:
                while True:
                    payload = json.dumps(
                        {"kafka": tap.snapshot(24), "pile": probe.snapshot()},
                        ensure_ascii=False)
                    self.wfile.write(f"data: {payload}\n\n".encode("utf-8"))
                    self.wfile.flush()
                    time.sleep(STREAM_INTERVAL)
            except (BrokenPipeError, ConnectionResetError):
                return

        def do_GET(self):                                   # noqa: N802
            route = self.path.split("?")[0].rstrip("/") or "/"

            if route in ("/", "/index.html", "/aftershock.html"):
                self._page()
            elif route == "/api/live":
                self._stream()
            elif route == "/api/kafka":
                self._json(tap.snapshot(60))
            elif route == "/api/pile":
                self._json(probe.snapshot())
            elif route == "/api/bundle":
                self._raw(cache.fetch(BUNDLE_PATH),
                          "application/json; charset=utf-8")
            elif route == "/api/model":
                self._raw(cache.fetch(MODEL_PATH),
                          "application/json; charset=utf-8")
            elif route == "/api/health":
                self._json({"kafka": tap.error is None,
                            "messages": tap.total,
                            "page": PAGE.exists()})
            else:
                self._json({"erreur": f"route inconnue : {route}"}, 404)

    return Handler


def main() -> int:
    settings = LakeSettings.from_environment()
    port = int(optional("DASHBOARD_PORT", "8000"))

    tap = Tap(settings.kafka_bootstrap, settings.topic_quakes)
    tap.start()

    probe = Probe(settings.webhdfs_url,
                  optional("SPARK_MASTER_UI", "http://spark-master:8080"))
    probe.start()

    cache = Cache(WebHdfs(settings.webhdfs_url))

    server = ThreadingHTTPServer(("0.0.0.0", port),
                                 handler_factory(tap, probe, cache))
    print(f"tableau de bord sur le port {port}")
    print(f"  topic   {settings.topic_quakes}")
    print(f"  lac     {settings.webhdfs_url}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        server.shutdown()
    return 0


if __name__ == "__main__":
    sys.exit(main())
