from __future__ import annotations

import json
import sys
from datetime import datetime, timezone

from pyspark.sql.utils import AnalysisException

from src.common.config import (GOLD_ROOT, ConfigurationError, LakeSettings,
                               optional)
from src.common.hdfs import WebHdfs
from src.common.spark import build_session
from src.ml import classify, regression
from src.ml.features import (CATEGORICAL_FEATURES, NUMERIC_FEATURES,
                             assert_no_leakage, excluded, missing_indicators,
                             split_by_time)

APPLICATION_NAME = "ml-revision-model"
DATASET_TABLE = f"{GOLD_ROOT}/magnitude_revision"
MODEL_ROOT = "/lake/models"
METRICS_TABLE = f"{GOLD_ROOT}/model_report"

MINIMUM_TEST_SIZE = 30


def show_regression(report: dict) -> None:
    zero = report["references"]["toujours_zero"]["mae"]
    print()
    print("  REGRESSION — de combien la magnitude va bouger")
    print(f"    MAE modele          : {report['modele']['mae']:.4f}")
    print(f"    MAE toujours zero   : {zero:.4f}")
    print(f"    gain sur reference  : {report['gain_sur_reference_pct']} %")
    print(f"    bat la reference    : {report['bat_la_reference']}")
    print(f"    accord de signe     : {report['accord_de_signe']}")
    print("    variables les plus explicatives :")
    for row in report["importances"][:6]:
        print(f"      {row['variable']:32s} {row['poids']:.4f}")


def show_classification(report: dict) -> None:
    print()
    print("  CLASSIFICATION — faut-il se mefier de ce chiffre")
    if "abandonne" in report:
        print(f"    abandonnee : {report['abandonne']}")
        return
    matrix = report["matrice"]
    reference = report["reference"]
    print(f"    aire sous ROC       : {report['aire_sous_roc']:.4f}"
          f"   (0.5 = hasard)")
    print(f"    precision / rappel  : {matrix['precision_pct']} % / "
          f"{matrix['rappel_pct']} %")
    print(f"    part positive test  : {reference['part_positive_test_pct']} %")
    print(f"    exactitude si l'on repond toujours stable : "
          f"{reference['exactitude_si_toujours_stable_pct']} %")


def main() -> int:
    try:
        assert_no_leakage()
        settings = LakeSettings.from_environment()
        session = build_session(APPLICATION_NAME)
    except ConfigurationError as error:
        print(error, file=sys.stderr)
        return 2

    seed = int(optional("MODEL_SEED", "20260827"))

    # Une ablation ne doit jamais ecraser les artefacts du modele de reference :
    # le site lit report.json, et un essai exploratoire le remplacerait en
    # silence par des chiffres tronques.
    label = optional("RUN_LABEL", "").strip()
    suffix = f"_{label}" if label else ""
    report_dir = f"{METRICS_TABLE}{suffix}"
    model_dir = f"{MODEL_ROOT}{suffix}"
    if label:
        print(f"  essai etiquete '{label}' -> {report_dir}")

    try:
        dataset = session.read.parquet(f"{settings.hdfs_uri}{DATASET_TABLE}")
    except AnalysisException:
        print("table Gold magnitude_revision absente", file=sys.stderr)
        session.stop()
        return 1

    prepared = missing_indicators(dataset).cache()
    train, test, frontier = split_by_time(prepared)
    train_size, test_size = train.count(), test.count()

    print("-" * 74)
    print(f"  echantillon total   : {prepared.count()}")
    print(f"  coupure temporelle  : {frontier}")
    print(f"  apprentissage       : {train_size}    test : {test_size}")
    print("-" * 74)

    if test_size < MINIMUM_TEST_SIZE:
        print(f"ECHEC : {test_size} exemples de test, insuffisant pour conclure",
              file=sys.stderr)
        session.stop()
        return 1

    shift_report, shift_model = regression.run(train, test, seed)
    class_report, class_model = classify.run(train, test, seed)

    report = {
        "genere_le": datetime.now(timezone.utc).isoformat(),
        "graine": seed,
        "coupure_temporelle": frontier,
        "taille_apprentissage": train_size,
        "taille_test": test_size,
        "variables_categorielles": list(CATEGORICAL_FEATURES),
        "variables_candidates": list(NUMERIC_FEATURES),
        "variables_ecartees": sorted(excluded()),
        "regression": shift_report,
        "classification": class_report,
    }

    base = settings.hdfs_uri
    shift_model.write().overwrite().save(f"{base}{model_dir}/revision_shift")
    if class_model is not None:
        class_model.write().overwrite().save(f"{base}{model_dir}/revision_flag")

    fs = WebHdfs(settings.webhdfs_url)
    fs.mkdirs(report_dir)
    fs.write(f"{report_dir}/report.json",
             json.dumps(report, indent=2, ensure_ascii=False).encode("utf-8"))

    show_regression(shift_report)
    show_classification(class_report)

    session.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
