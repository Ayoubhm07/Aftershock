#!/usr/bin/env bash
# PREUVE DE REJEU SANS DUPLICATION
#
# Table rase, declenchement, arret brutal d'Airflow en plein vol, redemarrage,
# relance, puis second rejeu sur un lac deja complet. Le lot doit etre
# identique a l'octet pres, sinon le script sort en erreur.
set -euo pipefail

# Git Bash reecrit tout argument commencant par / en chemin Windows avant de
# le passer a docker : /lake/bronze devient C:/Program Files/Git/lake/bronze,
# HDFS repond 'No FileSystem for scheme "C"', et un 2>/dev/null transforme
# l'echec en comptage a zero. La preuve se declarait alors reussie sur un lac
# qu'elle n'avait ni efface ni mesure.
export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL='*'

DAG=bronze_catalog_ingestion
CATALOG=/lake/bronze/source=usgs_catalog
KILL_AFTER=${KILL_AFTER:-20}
HEALTH_TIMEOUT=${HEALTH_TIMEOUT:-600}
IDLE_TIMEOUT=${IDLE_TIMEOUT:-900}

started=$(date +%s)

# Pas de 2>/dev/null global : une erreur HDFS doit se voir. Seules les
# commandes dont l'echec est attendu la redirigent, au cas par cas.
hdfs() { docker exec seisme-namenode hdfs dfs "$@"; }

listing() { hdfs -ls -R "$CATALOG" 2>/dev/null || true; }

# grep -c affiche deja "0" avant de sortir en erreur : un '|| echo 0' ferait
# imprimer la valeur deux fois. C'est '|| true' qu'il faut.
count_in_catalog() {
  local found
  found=$(listing | grep -c "$1" || true)
  echo "${found:-0}"
}

markers() { count_in_catalog '_SUCCESS'; }
payloads() { count_in_catalog 'events.geojson'; }
bytes() { hdfs -du -s "$CATALOG" 2>/dev/null | awk '{print $1}' || echo 0; }

# Aucune attente sans borne : une pile qui ne repart pas doit faire echouer la
# preuve, pas la faire pendre en silence. Airflow met plusieurs minutes a
# revenir sous charge, d'ou un plafond genereux plutot qu'absent.
wait_healthy() {
  local deadline=$((SECONDS + HEALTH_TIMEOUT))
  until [ "$(docker inspect seisme-airflow --format '{{.State.Health.Status}}' \
            2>/dev/null)" = healthy ]; do
    if [ "$SECONDS" -ge "$deadline" ]; then
      echo "      ECHEC : Airflow n'est pas revenu en ${HEALTH_TIMEOUT}s" >&2
      exit 1
    fi
    sleep 10
  done
}

wait_idle() {
  local deadline=$((SECONDS + IDLE_TIMEOUT))
  until ! docker exec seisme-airflow airflow dags list-runs -d "$DAG" -o plain \
        2>/dev/null | grep -qE 'running|queued'; do
    if [ "$SECONDS" -ge "$deadline" ]; then
      echo "      ECHEC : le DAG tourne encore apres ${IDLE_TIMEOUT}s" >&2
      exit 1
    fi
    sleep 5
  done
}

echo "=============================================================="
echo " PREUVE DE REJEU SANS DUPLICATION"
echo "=============================================================="

# Une preuve qui reussit sur un lac vide n'est pas une preuve : elle compare
# zero a zero. Ces deux assertions rendent le vide impossible a confondre avec
# le succes -- exactement le defaut que ce projet denonce ailleurs.
expect_empty() {
  local m p
  m=$(markers); p=$(payloads)
  if [ "$m" != "0" ] || [ "$p" != "0" ]; then
    echo "      ECHEC : la table rase a laisse $m marqueur(s), $p fichier(s)." >&2
    echo "              Les chemins sont-ils bien transmis a docker ?" >&2
    exit 1
  fi
}

expect_ingested() {
  local m p b
  m=$(markers); p=$(payloads); b=$(bytes)
  if [ "$m" -lt 1 ] || [ "$p" -lt 1 ] || [ "${b:-0}" -lt 1 ]; then
    echo "      ECHEC : rien n'a ete ingere ($m marqueurs, $p fichiers," >&2
    echo "              ${b:-0} octets). Comparer 0 a 0 passerait le test" >&2
    echo "              d'egalite sans rien prouver." >&2
    exit 1
  fi
}

echo
echo "[1/5] Table rase sur la couche Bronze du catalogue"
hdfs -rm -r -skipTrash "$CATALOG" >/dev/null 2>&1 || true
echo "      marqueurs=$(markers)  fichiers=$(payloads)"
expect_empty

echo
echo "[2/5] Declenchement, puis ARRET BRUTAL apres ${KILL_AFTER}s"
wait_healthy
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
wait_healthy
echo "      Airflow revenu, relance du DAG"
docker exec seisme-airflow airflow dags trigger "$DAG" >/dev/null
wait_idle

FINAL_MARKERS=$(markers)
FINAL_PAYLOADS=$(payloads)
FINAL_BYTES=$(bytes)
echo "      etat complet : marqueurs=$FINAL_MARKERS  fichiers=$FINAL_PAYLOADS"
expect_ingested

echo
echo "[4/5] Second rejeu, sur un lac deja complet"
docker exec seisme-airflow airflow dags trigger "$DAG" >/dev/null
wait_idle

REPLAY_MARKERS=$(markers)
REPLAY_PAYLOADS=$(payloads)
REPLAY_BYTES=$(bytes)
echo "      apres rejeu  : marqueurs=$REPLAY_MARKERS  fichiers=$REPLAY_PAYLOADS"
expect_ingested

echo
echo "[5/5] Verdict"
echo "--------------------------------------------------------------"
printf "  interrompu -> complet : %s -> %s marqueurs\n" "$PARTIAL_MARKERS" "$FINAL_MARKERS"
printf "  complet    -> rejeu   : %s -> %s marqueurs\n" "$FINAL_MARKERS" "$REPLAY_MARKERS"
printf "  fichiers   -> rejeu   : %s -> %s\n" "$FINAL_PAYLOADS" "$REPLAY_PAYLOADS"
printf "  octets     -> rejeu   : %s -> %s\n" "$FINAL_BYTES" "$REPLAY_BYTES"
printf "  duree totale          : %s s\n" "$(( $(date +%s) - started ))"
echo "--------------------------------------------------------------"

if [ "$FINAL_MARKERS" = "$REPLAY_MARKERS" ] \
   && [ "$FINAL_PAYLOADS" = "$REPLAY_PAYLOADS" ] \
   && [ "$FINAL_BYTES" = "$REPLAY_BYTES" ]; then
  echo "  SUCCES : le rejeu n'a rien duplique, pas un octet."
  exit 0
fi

echo "  ECHEC : le rejeu a modifie la couche Bronze." >&2
exit 1
