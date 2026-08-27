from __future__ import annotations

import json
import re
import sys

from pyspark.ml import PipelineModel
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

from src.common.config import (GOLD_ROOT, ConfigurationError, LakeSettings,
                               optional)
from src.common.hdfs import WebHdfs
from src.common.spark import build_session
from src.ml.classify import LABEL, add_label
from src.ml.features import (CATEGORICAL_FEATURES, missing_indicators,
                             split_by_time)

APPLICATION_NAME = "ml-export-model"
DATASET_TABLE = f"{GOLD_ROOT}/magnitude_revision"
MODEL_ROOT = "/lake/models"
EXPORT_PATH = f"{GOLD_ROOT}/model_export/trees.json"

WITNESS_ROWS = 40

SPLIT = re.compile(
    r"^(\s*)(If|Else)\s+\(feature (\d+) "
    r"(?:(<=|>) ([-\d.eE]+)|(not in|in) \{([^}]*)\})\)$")
LEAF = re.compile(r"^(\s*)Predict:\s*([-\d.eE]+)$")


def parse_tree(debug: str) -> dict:
    """Reconstruit l'arbre depuis toDebugString. PySpark n'expose pas les
    noeuds ; passer par le Java sous-jacent lierait l'export a une version
    precise de Spark, la representation texte est stable depuis 2.x."""
    lines = [line.rstrip() for line in debug.split("\n")[1:] if line.strip()]
    position = 0

    def node():
        nonlocal position
        line = lines[position]

        leaf = LEAF.match(line)
        if leaf:
            position += 1
            return {"valeur": float(leaf.group(2))}

        match = SPLIT.match(line)
        if not match:
            raise ValueError(f"ligne non reconnue : {line!r}")

        _, branch, feature, operator, threshold, membership, categories = \
            match.groups()
        if branch != "If":
            raise ValueError(f"branche inattendue : {line!r}")

        position += 1
        left = node()

        if position >= len(lines) or not lines[position].strip().startswith("Else"):
            raise ValueError(f"branche Else absente apres {line!r}")
        position += 1
        right = node()

        split = {"variable": int(feature), "gauche": left, "droite": right}
        if operator is not None:
            split["seuil"] = float(threshold)
        else:
            split["categories"] = sorted(
                float(token) for token in categories.split(",") if token.strip())
        return split

    root = node()
    if position != len(lines):
        raise ValueError(f"{len(lines) - position} ligne(s) non consommee(s)")
    return root


def describe_stages(pipeline: PipelineModel) -> dict:
    indexers, encoders, imputer, assembler = [], [], None, None

    for stage in pipeline.stages:
        name = type(stage).__name__
        if name == "StringIndexerModel":
            indexers.append({"colonne": stage.getInputCol(),
                             "modalites": list(stage.labels)})
        elif name == "OneHotEncoderModel":
            encoders.append({"colonne": stage.getInputCol(),
                             "categories": int(stage.categorySizes[0]),
                             "abandonne_la_derniere": bool(stage.getDropLast()),
                             "invalide_conserve":
                                 stage.getHandleInvalid() == "keep"})
        elif name == "ImputerModel":
            row = stage.surrogateDF.first().asDict()
            imputer = {"colonnes": list(stage.getInputCols()),
                       "medianes": {k: float(v) for k, v in row.items()}}
        elif name == "VectorAssembler":
            assembler = list(stage.getInputCols())

    return {"indexeurs": indexers, "encodeurs": encoders,
            "imputation": imputer, "assemblage": assembler}


def ensemble(model) -> dict:
    return {
        "arbres": [parse_tree(tree.toDebugString) for tree in model.trees],
        "poids": [float(weight) for weight in model.treeWeights],
        "nombre_de_variables": int(model.numFeatures),
    }


def witnesses(pipeline: PipelineModel, test, columns: list[str]) -> list[dict]:
    """Un jeu temoin : les memes lignes, avec la prediction de Spark. Le
    navigateur rejoue l'inference dessus et affiche l'ecart. Sans ce
    controle, un portage silencieusement faux passerait inapercu."""
    scored = pipeline.transform(test).select(
        *columns, F.col("prediction").alias("prediction_spark"))
    return [row.asDict() for row in scored.limit(WITNESS_ROWS).collect()]


def main() -> int:
    try:
        settings = LakeSettings.from_environment()
        session = build_session(APPLICATION_NAME)
    except ConfigurationError as error:
        print(error, file=sys.stderr)
        return 2

    base = settings.hdfs_uri
    try:
        dataset = session.read.parquet(f"{base}{DATASET_TABLE}")
        flag = PipelineModel.load(f"{base}{MODEL_ROOT}/revision_flag")
    except (AnalysisException, Exception) as error:         # noqa: BLE001
        print(f"modele ou jeu absent : {error}", file=sys.stderr)
        session.stop()
        return 1

    prepared = add_label(missing_indicators(dataset))
    _, test, _ = split_by_time(prepared)

    stages = describe_stages(flag)
    numeric = stages["imputation"]["colonnes"]
    raw_columns = numeric + list(CATEGORICAL_FEATURES) + [LABEL, "event_id",
                                                          "place"]

    classifier = flag.stages[-1]
    export = {
        "question": "la magnitude sera-t-elle relevee d'au moins 0.2",
        "etages": stages,
        "foret": ensemble(classifier),
        "temoins": witnesses(flag, test, raw_columns),
        "colonnes_temoins": raw_columns,
    }

    payload = json.dumps(export, ensure_ascii=False, separators=(",", ":"))
    fs = WebHdfs(settings.webhdfs_url)
    fs.mkdirs(f"{GOLD_ROOT}/model_export")
    fs.write(EXPORT_PATH, payload.encode("utf-8"))

    trees = export["foret"]["arbres"]
    print("-" * 70)
    print(f"  arbres exportes      : {len(trees)}")
    print(f"  variables assemblees : {export['foret']['nombre_de_variables']}")
    print(f"  temoins de controle  : {len(export['temoins'])}")
    print(f"  poids                : {len(payload) / 1024:.1f} Ko")
    print(f"  ecrit dans {EXPORT_PATH}")
    print("-" * 70)

    session.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
