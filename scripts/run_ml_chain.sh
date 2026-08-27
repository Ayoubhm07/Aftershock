#!/usr/bin/env bash
# Enchaine Silver -> Gold -> modele -> export du bundle de restitution.
# Chaque etape declare son budget Spark : le worker doit garder de quoi
# faire vivre le job de streaming, sinon son executor est tue.
set -uo pipefail

SRC="${1:?usage: run_ml_chain.sh <chemin absolu vers src>}"
CORES="${CORES_MAX:-2}"
MEMORY="${EXECUTOR_MEMORY:-1g}"

run() {
  local name="$1"
  local module="$2"
  echo "=============================================================="
  echo "  ${name}"
  echo "=============================================================="
  docker run --rm --network seisme_default --name "chain-${name}" \
    -v "${SRC}:/opt/app/src:ro" \
    -e SPARK_MASTER_URL=spark://spark-master:7077 \
    -e SPARK_MASTER_UI=http://spark-master:8080 \
    -e SPARK_DRIVER_HOST="chain-${name}" \
    -e HDFS_URI=hdfs://namenode:8020 \
    -e WEBHDFS_URL=http://namenode:9870 \
    -e CORES_MAX="${CORES}" \
    -e EXECUTOR_MEMORY="${MEMORY}" \
    -e PYTHONUNBUFFERED=1 \
    seisme-spark:latest python3 -m "${module}" 2>&1 \
    | grep -Ev 'WARN|INFO|^\[Stage|Setting default log level|To adjust logging'
  echo
}

run silver  src.silver.build_version_history
run dataset src.gold.build_revision_dataset
run curve   src.gold.build_alert_curve
run model   src.ml.train
run export  src.gold.export_site
