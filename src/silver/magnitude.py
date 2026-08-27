from __future__ import annotations

from dataclasses import dataclass

FAMILY_MOMENT = "moment"
FAMILY_LOCAL = "local"
FAMILY_DURATION = "duration"
FAMILY_BODY = "body_wave"
FAMILY_SURFACE = "surface_wave"
FAMILY_UNKNOWN = "inconnue"


@dataclass(frozen=True)
class MagnitudeScale:
    prefix: str
    family: str
    saturates_above: float
    label: str


SCALES: tuple[MagnitudeScale, ...] = (
    MagnitudeScale("mww", FAMILY_MOMENT, 10.0, "moment, tenseur W-phase"),
    MagnitudeScale("mwr", FAMILY_MOMENT, 10.0, "moment, regional"),
    MagnitudeScale("mwc", FAMILY_MOMENT, 10.0, "moment, centroide"),
    MagnitudeScale("mwb", FAMILY_MOMENT, 10.0, "moment, ondes de volume"),
    MagnitudeScale("mw", FAMILY_MOMENT, 10.0, "moment"),
    MagnitudeScale("ms_vx", FAMILY_SURFACE, 8.0, "ondes de surface"),
    MagnitudeScale("ms", FAMILY_SURFACE, 8.0, "ondes de surface"),
    MagnitudeScale("mb_lg", FAMILY_BODY, 6.5, "ondes de volume, Lg"),
    MagnitudeScale("mb", FAMILY_BODY, 6.5, "ondes de volume"),
    MagnitudeScale("ml", FAMILY_LOCAL, 6.5, "locale, dite de Richter"),
    MagnitudeScale("mlr", FAMILY_LOCAL, 6.5, "locale, regionale"),
    MagnitudeScale("md", FAMILY_DURATION, 5.0, "duree du signal"),
    MagnitudeScale("mh", FAMILY_DURATION, 5.0, "duree, historique"),
)

REFERENCE_FAMILY = FAMILY_MOMENT
UNKNOWN_SATURATION = 5.0


def classify(mag_type: str | None) -> MagnitudeScale:
    token = (mag_type or "").strip().lower()
    for scale in SCALES:
        if token.startswith(scale.prefix):
            return scale
    return MagnitudeScale(token or "?", FAMILY_UNKNOWN, UNKNOWN_SATURATION, "inconnue")


def family_of(mag_type: str | None) -> str:
    return classify(mag_type).family


def saturation_of(mag_type: str | None) -> float:
    return classify(mag_type).saturates_above


def is_saturated(mag_type: str | None, magnitude: float | None) -> bool:
    if magnitude is None:
        return False
    return magnitude >= saturation_of(mag_type)


FAMILY_LOOKUP: dict[str, str] = {}
SATURATION_LOOKUP: dict[str, float] = {}
for _scale in SCALES:
    FAMILY_LOOKUP[_scale.prefix] = _scale.family
    SATURATION_LOOKUP[_scale.prefix] = _scale.saturates_above
