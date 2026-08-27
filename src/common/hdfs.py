from __future__ import annotations

import os
import time

import requests

DEFAULT_TIMEOUT = (10, 300)
TRANSPORT_ATTEMPTS = 4
TRANSPORT_BACKOFF = 3.0


class HdfsError(RuntimeError):
    pass


def call(method, action: str, *args, **kwargs) -> requests.Response:
    """Un echec de transport n'est pas une reponse HTTP : sans cette
    conversion, une coupure reseau remonte en requests.ConnectionError et
    traverse tous les gestionnaires qui n'attrapent que HdfsError."""
    last: Exception | None = None
    for attempt in range(TRANSPORT_ATTEMPTS):
        try:
            return method(*args, **kwargs)
        except requests.RequestException as error:
            last = error
            if attempt + 1 < TRANSPORT_ATTEMPTS:
                time.sleep(TRANSPORT_BACKOFF * (attempt + 1))
    raise HdfsError(f"{action} : {type(last).__name__} apres "
                    f"{TRANSPORT_ATTEMPTS} tentatives ({last})")


class WebHdfs:
    def __init__(self, base_url: str | None = None, user: str = "hadoop") -> None:
        host = base_url or os.environ.get("WEBHDFS_URL", "http://namenode:9870")
        self.base = f"{host.rstrip('/')}/webhdfs/v1"
        self.user = user

    def _url(self, path: str, op: str, **params) -> str:
        query = "&".join(f"{k}={v}" for k, v in
                         {"op": op, "user.name": self.user, **params}.items())
        return f"{self.base}{path}?{query}"

    def _check(self, response: requests.Response, action: str) -> requests.Response:
        if response.status_code >= 400:
            raise HdfsError(f"{action} : HTTP {response.status_code} {response.text[:300]}")
        return response

    def exists(self, path: str) -> bool:
        return call(requests.get, f"stat {path}", self._url(path, "GETFILESTATUS"),
                    timeout=DEFAULT_TIMEOUT).status_code == 200

    def mkdirs(self, path: str) -> None:
        self._check(call(requests.put, f"mkdirs {path}",
                         self._url(path, "MKDIRS"), timeout=DEFAULT_TIMEOUT),
                    f"mkdirs {path}")

    def listdir(self, path: str) -> list[str]:
        response = call(requests.get, f"ls {path}",
                        self._url(path, "LISTSTATUS"), timeout=DEFAULT_TIMEOUT)
        if response.status_code == 404:
            return []
        self._check(response, f"ls {path}")
        return [e["pathSuffix"]
                for e in response.json()["FileStatuses"]["FileStatus"]]

    def content_summary(self, path: str) -> dict:
        """Un seul appel pour le nombre de fichiers et les octets d'une
        arborescence. Compter en descendant recursivement coute une requete
        HTTP par entree, ce qui rend toute sonde periodique inutilisable."""
        response = call(requests.get, f"summary {path}",
                        self._url(path, "GETCONTENTSUMMARY"),
                        timeout=DEFAULT_TIMEOUT)
        if response.status_code == 404:
            return {"fichiers": 0, "repertoires": 0, "octets": 0}
        self._check(response, f"summary {path}")
        payload = response.json()["ContentSummary"]
        return {
            "fichiers": payload.get("fileCount", 0),
            "repertoires": payload.get("directoryCount", 0),
            "octets": payload.get("length", 0),
        }

    def size(self, path: str) -> int:
        response = self._check(
            call(requests.get, f"stat {path}", self._url(path, "GETFILESTATUS"),
                 timeout=DEFAULT_TIMEOUT), f"stat {path}")
        return response.json()["FileStatus"]["length"]

    def write(self, path: str, payload: bytes, overwrite: bool = True) -> int:
        first = call(
            requests.put, f"CREATE {path}",
            self._url(path, "CREATE", overwrite=str(overwrite).lower()),
            allow_redirects=False, timeout=DEFAULT_TIMEOUT)
        if first.status_code not in (307, 201):
            raise HdfsError(f"CREATE {path} : HTTP {first.status_code} {first.text[:300]}")

        location = first.headers.get("Location")
        if location is None:
            raise HdfsError(f"CREATE {path} : aucun datanode indique par le namenode")

        self._check(
            call(requests.put, f"ecriture {path}", location, data=payload,
                 timeout=DEFAULT_TIMEOUT,
                 headers={"Content-Type": "application/octet-stream"}),
            f"ecriture {path}")
        return len(payload)

    def read(self, path: str) -> bytes:
        return self._check(
            call(requests.get, f"lecture {path}", self._url(path, "OPEN"),
                 timeout=DEFAULT_TIMEOUT), f"lecture {path}").content

    def delete(self, path: str, recursive: bool = True) -> None:
        call(requests.delete, f"suppression {path}",
             self._url(path, "DELETE", recursive=str(recursive).lower()),
             timeout=DEFAULT_TIMEOUT)

    def wait_ready(self, attempts: int = 60, delay: float = 3.0) -> None:
        import time
        last = "aucune reponse"
        for _ in range(attempts):
            try:
                if requests.get(self._url("/", "LISTSTATUS"),
                                timeout=(5, 15)).status_code == 200:
                    return
            except requests.RequestException as error:
                last = f"{type(error).__name__}: {error}"
            time.sleep(delay)
        raise HdfsError(f"HDFS injoignable apres {attempts} tentatives ({last})")
