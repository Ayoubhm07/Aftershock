from __future__ import annotations

import pendulum
from airflow.decorators import dag, task
from airflow.utils.trigger_rule import TriggerRule

from lake_datasets import BRONZE_CATALOG

CATALOG_FROM = "2025-01"
CATALOG_TO = "2025-12"
MIN_MAGNITUDE = 4.0


@dag(
    dag_id="bronze_catalog_ingestion",
    description="Depose les lots mensuels du catalogue USGS en Bronze",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["bronze", "batch"],
    default_args={"retries": 2, "retry_delay": pendulum.duration(minutes=2)},
)
def bronze_catalog_ingestion():

    @task
    def wait_for_hdfs() -> str:
        from src.common.hdfs import WebHdfs
        fs = WebHdfs()
        fs.wait_ready()
        return "hdfs disponible"

    @task
    def pending_months() -> list[dict]:
        from src.common.config import INGESTED_MARKER, LakeSettings
        from src.common.hdfs import WebHdfs
        from src.ingest.batch_to_bronze import months_between

        settings = LakeSettings.from_environment()
        fs = WebHdfs(settings.webhdfs_url)

        waiting = []
        for year, month in months_between(CATALOG_FROM, CATALOG_TO):
            directory = settings.bronze_catalog(year, month)
            if not fs.exists(f"{directory}/{INGESTED_MARKER}"):
                waiting.append({"year": year, "month": month})
        print(f"{len(waiting)} mois a ingerer")
        return waiting

    @task(max_active_tis_per_dag=2)
    def ingest(window: dict) -> str:
        from src.common.config import LakeSettings
        from src.common.hdfs import WebHdfs
        from src.common.usgs import CatalogSlice
        from src.ingest.batch_to_bronze import ingest_month

        settings = LakeSettings.from_environment()
        fs = WebHdfs(settings.webhdfs_url)
        verdict = ingest_month(
            fs, settings,
            CatalogSlice(window["year"], window["month"], MIN_MAGNITUDE),
            force=False)
        print(verdict)
        if verdict.startswith("ECHEC"):
            raise RuntimeError(verdict)
        return verdict

    @task(outlets=[BRONZE_CATALOG], trigger_rule=TriggerRule.ALL_DONE)
    def publish(results: list[str] | None = None) -> str:
        outcomes = results or []
        ingested = sum(1 for r in outcomes if r.startswith("INGERE"))
        skipped = sum(1 for r in outcomes if r.startswith("DEJA"))
        summary = f"{ingested} ingere(s), {skipped} deja present(s)"
        print(summary)
        print("couche Bronze a jour, Dataset emis")
        return summary

    ready = wait_for_hdfs()
    windows = pending_months()
    ready >> windows
    publish(ingest.expand(window=windows))


bronze_catalog_ingestion()
