from __future__ import annotations

import argparse
import hashlib
import json
import random
import sys
from datetime import datetime, timezone

from src.common.config import (BRONZE_ROOT, INGESTED_MARKER, SOURCE_CATALOG,
                               VERSIONS_BATCH_SIZE, LakeSettings, optional)
from src.common.hdfs import HdfsError, WebHdfs
from src.common.usgs import (UsgsUnavailable, fetch_versions, identifiers,
                             project_versions)

AUTOMATIC_PREFIX = "usauto"
CONTROL_SAMPLE = 1000
SELECTION_SEED = 20260827


def catalog_events(fs: WebHdfs) -> list[dict]:
    root = f"{BRONZE_ROOT}/source={SOURCE_CATALOG}"
    events: list[dict] = []
    for year in sorted(fs.listdir(root)):
        for month in sorted(fs.listdir(f"{root}/{year}")):
            directory = f"{root}/{year}/{month}"
            if INGESTED_MARKER not in fs.listdir(directory):
                continue
            document = json.loads(fs.read(f"{directory}/events.geojson"))
            events.extend(document.get("features", []))
    return events


def select_events(events: list[dict]) -> tuple[list[str], list[str]]:
    traced, untraced = [], []
    for feature in events:
        known = identifiers(feature.get("properties", {}))
        target = feature.get("id")
        if not target:
            continue
        if any(token.startswith(AUTOMATIC_PREFIX) for token in known):
            traced.append(target)
        else:
            untraced.append(target)

    rng = random.Random(SELECTION_SEED)
    control = sorted(rng.sample(untraced, min(CONTROL_SAMPLE, len(untraced))))
    return sorted(set(traced)), control


def already_ingested(fs: WebHdfs, directory: str) -> bool:
    return INGESTED_MARKER in fs.listdir(directory)


def harvest_batch(fs: WebHdfs, settings: LakeSettings, index: int,
                  members: list[tuple[str, str]], force: bool) -> str:
    directory = settings.bronze_versions(index)
    label = f"lot {index:04d}"

    if not force and already_ingested(fs, directory):
        return f"DEJA INGERE  {label}"

    lines: list[bytes] = []
    failures = 0
    for event_id, cohort in members:
        try:
            document = fetch_versions(event_id)
        except (UsgsUnavailable, ValueError):
            failures += 1
            continue
        record = project_versions(event_id, document)
        record["cohort"] = cohort
        lines.append(json.dumps(record, separators=(",", ":")).encode("utf-8"))

    if not lines:
        return f"ECHEC        {label} : aucune reponse exploitable"

    payload = b"\n".join(lines) + b"\n"
    fs.mkdirs(directory)
    fs.write(f"{directory}/versions.jsonl", payload)

    written = fs.size(f"{directory}/versions.jsonl")
    if written != len(payload):
        fs.delete(f"{directory}/versions.jsonl")
        return (f"ECHEC        {label} : {written} octets ecrits "
                f"pour {len(payload)} attendus")

    marker = {
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "batch": index,
        "requested": len(members),
        "events": len(lines),
        "failures": failures,
        "bytes": len(payload),
        "sha256": hashlib.sha256(payload).hexdigest(),
    }
    fs.write(f"{directory}/{INGESTED_MARKER}",
             json.dumps(marker, indent=2).encode("utf-8"))

    return (f"INGERE       {label}  {len(lines):>3}/{len(members)} evenements  "
            f"{len(payload)/1024:8.1f} Ko")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Depose l'historique des versions USGS en Bronze.")
    parser.add_argument("--batch-size", type=int, default=VERSIONS_BATCH_SIZE)
    parser.add_argument("--max-batches", type=int,
                        default=int(optional("VERSIONS_MAX_BATCHES", "0")))
    parser.add_argument("--force", action="store_true")
    arguments = parser.parse_args()

    settings = LakeSettings.from_environment()
    fs = WebHdfs(settings.webhdfs_url)
    fs.wait_ready()

    events = catalog_events(fs)
    if not events:
        print("catalogue Bronze vide, lancer bronze_catalog_ingestion d'abord",
              file=sys.stderr)
        return 1

    traced, control = select_events(events)
    members = ([(event_id, "traced") for event_id in traced]
               + [(event_id, "control") for event_id in control])

    print(f"catalogue : {len(events)} evenements")
    print(f"  cohorte tracee  : {len(traced)} (identifiant automatique conserve)")
    print(f"  cohorte temoin  : {len(control)} (tirage sans remise, graine "
          f"{SELECTION_SEED})")

    batches = [members[i:i + arguments.batch_size]
               for i in range(0, len(members), arguments.batch_size)]
    if arguments.max_batches > 0:
        batches = batches[:arguments.max_batches]
    print(f"  {len(batches)} lots de {arguments.batch_size}")

    failures = 0
    for index, batch in enumerate(batches):
        try:
            verdict = harvest_batch(fs, settings, index, batch, arguments.force)
        except HdfsError as error:
            verdict = f"ECHEC        lot {index:04d} : {error}"
        print(f"  {verdict}", flush=True)
        if verdict.startswith("ECHEC"):
            failures += 1

    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
