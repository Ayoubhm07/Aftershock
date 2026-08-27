from __future__ import annotations

import calendar
from dataclasses import dataclass
from datetime import date

import requests

LIVE_HOUR = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson"
LIVE_DAY = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson"
CATALOG = "https://earthquake.usgs.gov/fdsnws/event/1/query"

REQUEST_TIMEOUT = (10, 120)
CATALOG_PAGE_LIMIT = 20000


class UsgsUnavailable(RuntimeError):
    pass


@dataclass(frozen=True)
class CatalogSlice:
    year: int
    month: int
    min_magnitude: float

    @property
    def start(self) -> str:
        return date(self.year, self.month, 1).isoformat()

    @property
    def end(self) -> str:
        last = calendar.monthrange(self.year, self.month)[1]
        return date(self.year, self.month, last).isoformat()

    @property
    def label(self) -> str:
        return f"{self.year:04d}-{self.month:02d}"


def fetch_live(url: str = LIVE_HOUR) -> bytes:
    try:
        response = requests.get(url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
    except requests.RequestException as error:
        raise UsgsUnavailable(f"flux temps reel indisponible : {error}") from error
    return response.content


def fetch_catalog(window: CatalogSlice) -> bytes:
    parameters = {
        "format": "geojson",
        "starttime": window.start,
        "endtime": window.end,
        "minmagnitude": window.min_magnitude,
        "orderby": "time-asc",
        "limit": CATALOG_PAGE_LIMIT,
    }
    try:
        response = requests.get(CATALOG, params=parameters, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
    except requests.RequestException as error:
        raise UsgsUnavailable(
            f"catalogue indisponible pour {window.label} : {error}") from error
    return response.content


def identifiers(properties: dict) -> list[str]:
    raw = properties.get("ids") or ""
    return [token for token in raw.strip(",").split(",") if token]


def event_key(feature: dict) -> str:
    known = identifiers(feature.get("properties", {}))
    known.append(feature.get("id", ""))
    return min(token for token in sorted(set(known)) if token)


DETAIL = "https://earthquake.usgs.gov/fdsnws/event/1/query"

RETAINED_PRODUCTS = ("origin",)


def fetch_versions(event_id: str) -> dict:
    parameters = {
        "eventid": event_id,
        "format": "geojson",
        "includesuperseded": "true",
    }
    try:
        response = requests.get(DETAIL, params=parameters, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
    except requests.RequestException as error:
        raise UsgsUnavailable(
            f"historique indisponible pour {event_id} : {error}") from error
    return response.json()


def project_versions(event_id: str, document: dict) -> dict:
    properties = document.get("properties", {})
    products = properties.get("products", {})
    return {
        "event_id": event_id,
        "event_time": properties.get("time"),
        "current_magnitude": properties.get("mag"),
        "current_magnitude_type": properties.get("magType"),
        "place": properties.get("place"),
        "products": {name: products.get(name, []) for name in RETAINED_PRODUCTS},
    }
