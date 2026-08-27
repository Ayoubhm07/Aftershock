from __future__ import annotations

import time

from pyspark.sql import functions as F

from src.common.config import LakeSettings
from src.common.spark import build_session
from src.silver.build_version_history import (completed_batches,
                                              explode_origins, rank_versions)
from src.silver.versions import VERSIONS_DOCUMENT

settings = LakeSettings.from_environment()
session = build_session("profile-silver")

paths = completed_batches(settings)
print(f"{len(paths)} lots")


def chrono(label, action):
    start = time.time()
    result = action()
    print(f"  {label:44s} {time.time() - start:8.1f} s   -> {result}")
    return result


raw = session.read.schema(VERSIONS_DOCUMENT).json(paths)
chrono("1. lecture JSON + count", raw.count)

chrono("2. taille du tableau origin",
       lambda: raw.select(F.sum(F.size("products.origin"))).first()[0])

exploded = raw.select("event_id", F.explode("products.origin").alias("origin"))
chrono("3. explode seul + count", exploded.count)

projected = explode_origins(raw)
chrono("4. explode + 19 lectures de Map + count", projected.count)

cached = projected.cache()
chrono("5. meme chose, mise en cache", cached.count)

ranked = rank_versions(cached)
chrono("6. fenetres sur le cache + count", ranked.count)

chrono("7. collecte d'une ligne", lambda: len(ranked.limit(1).collect()))

session.stop()
