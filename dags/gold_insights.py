from __future__ import annotations

import pendulum
from airflow.decorators import dag
from airflow.providers.docker.operators.docker import DockerOperator

from lake_datasets import GOLD_TABLES, SILVER_EVENTS

SPARK_IMAGE = "seisme-spark:latest"
NETWORK = "seisme_default"

JOB_ENVIRONMENT = {
    "SPARK_MASTER_URL": "spark://spark-master:7077",
    "SPARK_MASTER_UI": "http://spark-master:8080",
    "SPARK_DRIVER_HOST": "gold-job",
    "HDFS_URI": "hdfs://namenode:8020",
    "WEBHDFS_URL": "http://namenode:9870",
    "CORES_MAX": "3",
    "EXECUTOR_MEMORY": "2g",
    "PYTHONUNBUFFERED": "1",
}


@dag(
    dag_id="gold_insights",
    description="Calcule les tables de restitution a partir de Silver",
    schedule=[SILVER_EVENTS],
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["gold", "spark"],
    default_args={"retries": 1, "retry_delay": pendulum.duration(minutes=3)},
)
def gold_insights():
    DockerOperator(
        task_id="build_gold",
        image=SPARK_IMAGE,
        container_name="gold-job",
        api_version="auto",
        auto_remove="force",
        command="python3 -m src.gold.build_gold",
        network_mode=NETWORK,
        environment=JOB_ENVIRONMENT,
        mount_tmp_dir=False,
        docker_url="unix://var/run/docker.sock",
        outlets=[GOLD_TABLES],
    )


gold_insights()
