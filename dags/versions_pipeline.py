from __future__ import annotations

import pendulum
from airflow.decorators import dag
from airflow.providers.docker.operators.docker import DockerOperator

from lake_datasets import BRONZE_VERSIONS, GOLD_MODEL, SILVER_VERSIONS

SPARK_IMAGE = "seisme-spark:latest"
APP_IMAGE = "seisme-app:latest"
NETWORK = "seisme_default"

LAKE = {
    "HDFS_URI": "hdfs://namenode:8020",
    "WEBHDFS_URL": "http://namenode:9870",
    "PYTHONUNBUFFERED": "1",
}

# Le worker offre 6 coeurs et 5 Go ; le job de streaming en tient 2 et 1 Go en
# permanence. Depasser ce reste tue son executor sans que le conteneur sorte.
SPARK = {
    **LAKE,
    "SPARK_MASTER_URL": "spark://spark-master:7077",
    "SPARK_MASTER_UI": "http://spark-master:8080",
    "CORES_MAX": "3",
    "EXECUTOR_MEMORY": "2g",
}


def spark_task(task_id: str, module: str, **kwargs) -> DockerOperator:
    return DockerOperator(
        task_id=task_id,
        image=SPARK_IMAGE,
        container_name=f"versions-{task_id}",
        api_version="auto",
        auto_remove="force",
        command=f"python3 -m {module}",
        network_mode=NETWORK,
        environment={**SPARK, "SPARK_DRIVER_HOST": f"versions-{task_id}"},
        mount_tmp_dir=False,
        docker_url="unix://var/run/docker.sock",
        **kwargs,
    )


@dag(
    dag_id="versions_pipeline",
    description="Historique des versions USGS : Bronze, Silver, Gold, modele",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["versions", "spark", "mllib"],
    default_args={"retries": 1, "retry_delay": pendulum.duration(minutes=5)},
)
def versions_pipeline():
    # La recolte est bornee par le reseau, pas par le calcul : elle appelle
    # l'API une fois par seisme. Aucun job Spark ne doit tourner en meme temps,
    # la contention a multiplie par quatorze la duree du job Silver.
    harvest = DockerOperator(
        task_id="harvest",
        image=APP_IMAGE,
        container_name="versions-harvest",
        api_version="auto",
        auto_remove="force",
        command="python -m src.ingest.versions_to_bronze",
        network_mode=NETWORK,
        environment=LAKE,
        mount_tmp_dir=False,
        docker_url="unix://var/run/docker.sock",
        outlets=[BRONZE_VERSIONS],
    )

    silver = spark_task("silver", "src.silver.build_version_history",
                        outlets=[SILVER_VERSIONS])
    dataset = spark_task("dataset", "src.gold.build_revision_dataset")
    curve = spark_task("curve", "src.gold.build_alert_curve")
    model = spark_task("model", "src.ml.train", outlets=[GOLD_MODEL])
    export = spark_task("export", "src.gold.export_site")

    harvest >> silver >> dataset >> curve >> model >> export


versions_pipeline()
