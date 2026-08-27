from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import date, datetime, timezone

from src.common.config import INGESTED_MARKER, LakeSettings, optional
from src.common.hdfs import HdfsError, WebHdfs
from src.common.usgs import (CATALOG_PAGE_LIMIT, CatalogSlice,
                             UsgsUnavailable, fetch_catalog)

DEFAULT_MIN_MAGNITUDE = 4.0


def months_between(first: str, last: str) -> list[tuple[int, int]]:
    start = datetime.strptime(first, "%Y-%m").date().replace(day=1)
    end = datetime.strptime(last, "%Y-%m").date().replace(day=1)
    months: list[tuple[int, int]] = []
    cursor = start
    while cursor <= end:
        months.append((cursor.year, cursor.month))
        cursor = (cursor.replace(year=cursor.year + 1, month=1)
                  if cursor.month == 12
                  else cursor.replace(month=cursor.month + 1))
    return months


def already_ingested(fs: WebHdfs, directory: str) -> bool:
    return fs.exists(f"{directory}/{INGESTED_MARKER}")


def ingest_month(fs: WebHdfs, settings: LakeSettings, window: CatalogSlice,
                 force: bool) -> str:
    directory = settings.bronze_catalog(window.year, window.month)

    if not force and already_ingested(fs, directory):
        return f"DEJA INGERE  {window.label}"

    payload = fetch_catalog(window)
    document = json.loads(payload)
    count = len(document.get("features", []))
    limit = document.get("metadata", {}).get("limit", CATALOG_PAGE_LIMIT)

    if count >= limit:
        return (f"ECHEC        {window.label} : {count} evenements atteignent la "
                f"limite de {limit}, le lot serait tronque en silence")

    fs.mkdirs(directory)
    fs.write(f"{directory}/events.geojson", payload)

    written = fs.size(f"{directory}/events.geojson")
    if written != len(payload):
        fs.delete(f"{directory}/events.geojson")
        return (f"ECHEC        {window.label} : {written} octets ecrits "
                f"pour {len(payload)} attendus")

    marker = {
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "period": window.label,
        "min_magnitude": window.min_magnitude,
        "events": count,
        "bytes": len(payload),
        "sha256": hashlib.sha256(payload).hexdigest(),
    }
    fs.write(f"{directory}/{INGESTED_MARKER}",
             json.dumps(marker, indent=2).encode("utf-8"))

    return f"INGERE       {window.label}  {count:>6} evenements  {len(payload)/1024:8.1f} Ko"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Depose des lots mensuels du catalogue USGS en Bronze.")
    parser.add_argument("--from-month", default=optional("BATCH_FROM", "2025-01"))
    parser.add_argument("--to-month", default=optional("BATCH_TO", "2025-12"))
    parser.add_argument("--min-magnitude", type=float,
                        default=float(optional("BATCH_MIN_MAGNITUDE",
                                               str(DEFAULT_MIN_MAGNITUDE))))
    parser.add_argument("--force", action="store_true")
    arguments = parser.parse_args()

    settings = LakeSettings.from_environment()
    fs = WebHdfs(settings.webhdfs_url)
    fs.wait_ready()

    months = months_between(arguments.from_month, arguments.to_month)
    print(f"{len(months)} mois demandes, M>={arguments.min_magnitude}")

    failures = 0
    for year, month in months:
        window = CatalogSlice(year, month, arguments.min_magnitude)
        try:
            verdict = ingest_month(fs, settings, window, arguments.force)
        except (UsgsUnavailable, HdfsError, json.JSONDecodeError) as error:
            verdict = f"ECHEC        {window.label} : {error}"
        print(f"  {verdict}")
        if verdict.startswith("ECHEC"):
            failures += 1

    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
