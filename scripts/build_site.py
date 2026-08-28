from __future__ import annotations

import json
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from src.common.config import GOLD_ROOT  # noqa: E402
from src.common.hdfs import HdfsError, WebHdfs  # noqa: E402

BUNDLE_PATH = f"{GOLD_ROOT}/site_bundle.json"
MODEL_PATH = f"{GOLD_ROOT}/model_export/trees.json"

ROOT = pathlib.Path(__file__).resolve().parents[1]
TEMPLATE = ROOT / "site" / "app.template.html"
APP = ROOT / "site" / "app.js"
VENDOR = ROOT / "site" / "vendor"
OUTPUT = ROOT / "site" / "aftershock.html"

# Les scripts sont inlines dans cet ordre : la CSP d'un artefact publie bloque
# tout hote externe, donc aucune balise src ne peut sortir de la page.
SCRIPTS = (
    ("/*__THREE__*/", "three.min.js", "three.js", False),
    ("/*__REACT__*/", "react.min.js", "React", True),
    ("/*__REACTDOM__*/", "react-dom.min.js", "React DOM", True),
    ("/*__HTM__*/", "htm.min.js", "htm", True),
)
LAND_FILE = VENDOR / "land.compact.json"


class BuildError(RuntimeError):
    pass


def inject(page: str, opening: str, closing: str | None, payload: str) -> str:
    """Remplace ce qui se trouve entre deux marqueurs. Une insertion naive par
    str.replace laisserait le contenu precedent en place lors d'une seconde
    construction, et la page grossirait a chaque passage."""
    start = page.find(opening)
    if start < 0:
        raise BuildError(f"marqueur {opening} absent du gabarit")
    head = start + len(opening)
    if closing is None:
        return page[:head] + payload + page[head:]
    stop = page.find(closing, head)
    if stop < 0:
        raise BuildError(f"marqueur {closing} absent du gabarit")
    return page[:head] + payload + page[stop:]


def fetch(fs: WebHdfs, path: str, label: str) -> str:
    try:
        payload = fs.read(path)
    except HdfsError as error:
        print(f"  {label:22s} absent ({error})", file=sys.stderr)
        return "null"
    try:
        parsed = json.loads(payload)
    except ValueError:
        print(f"  {label:22s} illisible", file=sys.stderr)
        return "null"
    compact = json.dumps(parsed, ensure_ascii=False, separators=(",", ":"))
    print(f"  {label:22s} {len(compact) / 1024:8.1f} Ko")
    return compact


def main() -> int:
    for required in (TEMPLATE, APP):
        if not required.exists():
            print(f"fichier absent : {required}", file=sys.stderr)
            return 1

    fs = WebHdfs()
    print("-" * 58)
    bundle = fetch(fs, BUNDLE_PATH, "donnees Gold")
    model = fetch(fs, MODEL_PATH, "modele exporte")

    if LAND_FILE.exists():
        land = LAND_FILE.read_text(encoding="utf-8")
        print(f"  cotes du monde         {len(land) / 1024:8.1f} Ko")
    else:
        land = "null"
        print("  cotes du monde         absentes, globe sans continents",
              file=sys.stderr)

    libraries: dict[str, str] = {}
    for marker, filename, label, required in SCRIPTS:
        source = VENDOR / filename
        if not source.exists():
            if required:
                print(f"  {label:22s} ABSENT — la page ne demarrera pas",
                      file=sys.stderr)
                return 1
            print(f"  {label:22s} absent, la vue concernee sera desactivee",
                  file=sys.stderr)
            libraries[marker] = ""
            continue
        content = source.read_text(encoding="utf-8")
        libraries[marker] = content
        print(f"  {label:22s} {len(content) / 1024:8.1f} Ko")

    application = APP.read_text(encoding="utf-8")
    print(f"  application            {len(application) / 1024:8.1f} Ko")

    try:
        page = TEMPLATE.read_text(encoding="utf-8")
        page = inject(page, "/*__BUNDLE__*/", "/*__BUNDLE_END__*/", bundle)
        page = inject(page, "/*__MODEL__*/", "/*__MODEL_END__*/", model)
        page = inject(page, "/*__LAND__*/", "/*__LAND_END__*/", land)
        for marker, _, _, _ in SCRIPTS:
            page = inject(page, marker, None, libraries[marker])
        page = inject(page, "/*__APP__*/", None, application)
    except BuildError as error:
        print(error, file=sys.stderr)
        return 1

    OUTPUT.write_text(page, encoding="utf-8")

    weight = len(page.encode("utf-8")) / 1024
    print("-" * 58)
    print(f"  page       {weight:8.1f} Ko -> {OUTPUT}")
    if weight > 16 * 1024:
        print("  ATTENTION : au-dela de la limite de 16 Mo d'un artefact",
              file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
