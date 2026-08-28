"""Compacte les polygones terrestres Natural Earth pour le globe.

Le GeoJSON d'origine porte une enveloppe de features, des proprietes inutiles
ici, et des coordonnees a sept decimales — soit une precision centimetrique
pour un globe qui fait 500 pixels. On garde des anneaux nus arrondis au
centieme de degre, ce qui vaut environ un kilometre.
"""
from __future__ import annotations

import json
import pathlib
import sys

SOURCE = pathlib.Path("site/vendor/land.json")
TARGET = pathlib.Path("site/vendor/land.compact.json")

DECIMALS = 2
MINIMUM_POINTS = 4


def rings(geometry: dict) -> list[list]:
    kind = geometry.get("type")
    if kind == "Polygon":
        return geometry.get("coordinates", [])
    if kind == "MultiPolygon":
        return [ring for polygon in geometry.get("coordinates", [])
                for ring in polygon]
    return []


def compact(ring: list) -> list[list[float]]:
    seen: list[list[float]] = []
    for point in ring:
        rounded = [round(float(point[0]), DECIMALS),
                   round(float(point[1]), DECIMALS)]
        # Arrondir cree des doublons consecutifs : les garder tracerait des
        # segments de longueur nulle et alourdirait le fichier pour rien.
        if seen and seen[-1] == rounded:
            continue
        seen.append(rounded)
    return seen


def main() -> int:
    if not SOURCE.exists():
        print(f"source absente : {SOURCE}", file=sys.stderr)
        return 1

    document = json.loads(SOURCE.read_text(encoding="utf-8"))
    kept: list[list[list[float]]] = []

    for feature in document.get("features", []):
        for ring in rings(feature.get("geometry", {})):
            reduced = compact(ring)
            if len(reduced) >= MINIMUM_POINTS:
                kept.append(reduced)

    payload = json.dumps(kept, separators=(",", ":"))
    TARGET.write_text(payload, encoding="utf-8")

    before = SOURCE.stat().st_size
    points = sum(len(ring) for ring in kept)
    print(f"  anneaux conserves : {len(kept)}")
    print(f"  points            : {points}")
    print(f"  {before / 1024:.1f} Ko -> {len(payload) / 1024:.1f} Ko "
          f"({100 * len(payload) / before:.0f} %)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
