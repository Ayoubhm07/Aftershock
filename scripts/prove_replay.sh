#!/usr/bin/env bash
set -euo pipefail

DAG=bronze_catalog_ingestion
CATALOG=/lake/bronze/source=usgs_catalog
KILL_AFTER=${KILL_AFTER:-20}

hdfs() { docker exec seisme-namenode hdfs dfs "$@" 2>/dev/null; }
markers() { hdfs -ls -R "$CATALOG" | grep -c '_SUCCESS' || echo 0; }
payloads() { hdfs -ls -R "$CATALOG" | grep -c 'events.geojson' || echo 0; }
bytes() { hdfs -du -s "$CATALOG" | awk '{print $1}' || echo 0; }

wait_idle() {
  until ! docker exec seisme-airflow airflow dags list-runs -d "$DAG" -o plain 2>/dev/null \
      | grep -qE 'running|queued'; do sleep 5; done
}

echo "=============================================================="
echo " PREUVE DE REJEU SANS DUPLICATION"
echo "=============================================================="

echo
echo "[1/5] Table rase sur la couche Bronze du catalogue"
hdfs -rm -r -skipTrash "$CATALOG" >/dev/null 2>&1 || true
echo "      marqueurs=$(markers)  fichiers=$(payloads)"

echo
echo "[2/5] Declenchement, puis ARRET BRUTAL apres ${KILL_AFTER}s"
docker exec seisme-airflow airflow dags unpause "$DAG" >/dev/null 2>&1 || true
docker exec seisme-airflow airflow dags trigger "$DAG" >/dev/null
sleep "$KILL_AFTER"
docker kill seisme-airflow >/dev/null
echo "      Airflow tue en plein vol"

PARTIAL_MARKERS=$(markers)
PARTIAL_PAYLOADS=$(payloads)
echo "      etat partiel : marqueurs=$PARTIAL_MARKERS  fichiers=$PARTIAL_PAYLOADS"

echo
echo "[3/5] Redemarrage et relance"
docker compose start airflow >/dev/null
until [ "$(docker inspect seisme-airflow --format '{{.State.Health.Status}}' 2>/dev/null)" = healthy ]; do
  sleep 10
done
docker exec seisme-airflow airflow dags trigger "$DAG" >/dev/null
wait_idle

FINAL_MARKERS=$(markers)
FINAL_PAYLOADS=$(payloads)
FINAL_BYTES=$(bytes)
echo "      etat complet : marqueurs=$FINAL_MARKERS  fichiers=$FINAL_PAYLOADS"

echo
echo "[4/5] Second rejeu, sur un lac deja complet"
docker exec seisme-airflow airflow dags trigger "$DAG" >/dev/null
wait_idle

REPLAY_MARKERS=$(markers)
REPLAY_PAYLOADS=$(payloads)
REPLAY_BYTES=$(bytes)
echo "      apres rejeu  : marqueurs=$REPLAY_MARKERS  fichiers=$REPLAY_PAYLOADS"

echo
echo "[5/5] Verdict"
echo "--------------------------------------------------------------"
printf "  interrompu -> complet : %s -> %s marqueurs\n" "$PARTIAL_MARKERS" "$FINAL_MARKERS"
printf "  complet    -> rejeu   : %s -> %s marqueurs\n" "$FINAL_MARKERS" "$REPLAY_MARKERS"
printf "  octets     -> rejeu   : %s -> %s\n" "$FINAL_BYTES" "$REPLAY_BYTES"
echo "--------------------------------------------------------------"

if [ "$FINAL_MARKERS" = "$REPLAY_MARKERS" ] \
   && [ "$FINAL_PAYLOADS" = "$REPLAY_PAYLOADS" ] \
   && [ "$FINAL_BYTES" = "$REPLAY_BYTES" ]; then
  echo "  SUCCES : le rejeu n'a rien duplique, pas un octet."
  exit 0
fi

echo "  ECHEC : le rejeu a modifie la couche Bronze." >&2
exit 1
