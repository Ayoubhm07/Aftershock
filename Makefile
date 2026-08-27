COMPOSE := docker compose

.PHONY: up down clean logs bronze replay hdfs airflow-ui help

help:
	@echo "make up        demarre HDFS, Kafka, Spark et Airflow"
	@echo "make bronze    declenche le DAG d'ingestion batch"
	@echo "make replay    PREUVE : tue un DAG en plein vol et relance"
	@echo "make hdfs      arborescence de la couche Bronze"
	@echo "make logs      logs de tous les services"
	@echo "make down      arret    |  make clean : arret + purge des volumes"

up:
	$(COMPOSE) up -d --build

bronze:
	$(COMPOSE) exec -T airflow airflow dags unpause bronze_catalog_ingestion
	$(COMPOSE) exec -T airflow airflow dags trigger bronze_catalog_ingestion

replay:
	@bash scripts/prove_replay.sh

hdfs:
	@$(COMPOSE) exec -T namenode hdfs dfs -du -h /lake/bronze
	@$(COMPOSE) exec -T namenode bash -c "hdfs dfs -ls -R /lake/bronze | head -30"

logs:
	$(COMPOSE) logs --tail=50

down:
	$(COMPOSE) down

clean:
	$(COMPOSE) down -v --remove-orphans
