COMPOSE := docker compose

.PHONY: up down clean logs bronze replay hdfs airflow-ui help versions chain site

help:
	@echo "make up        demarre HDFS, Kafka, Spark et Airflow"
	@echo "make bronze    declenche le DAG d'ingestion batch"
	@echo "make replay    PREUVE : tue un DAG en plein vol et relance"
	@echo "make versions  recolte l'historique des versions USGS (long, seul)"
	@echo "make chain     Silver -> Gold -> modele -> bundle de restitution"
	@echo "make site      injecte le bundle dans la page aftershock.html"
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

versions:
	@echo "ATTENTION : ne rien lancer d'autre pendant la recolte."
	@echo "La contention reseau multiplie par 14 la duree des jobs Spark."
	docker run --rm --network seisme_default --name versions-harvest 	  -v "$(CURDIR)/src:/opt/app/src:ro" 	  -e WEBHDFS_URL=http://namenode:9870 -e HDFS_URI=hdfs://namenode:8020 	  -e PYTHONUNBUFFERED=1 	  seisme-app:latest python -m src.ingest.versions_to_bronze

chain:
	@bash scripts/run_ml_chain.sh "$(CURDIR)/src"

site:
	docker run --rm --network seisme_default 	  -v "$(CURDIR):/opt/app:rw" -w /opt/app 	  -e WEBHDFS_URL=http://namenode:9870 -e PYTHONPATH=/opt/app 	  seisme-app:latest python scripts/build_site.py

travel:
	docker run --rm --network seisme_default --name time-travel 	  -v "$(CURDIR)/src:/opt/app/src:ro" 	  -e SPARK_MASTER_URL=spark://spark-master:7077 	  -e SPARK_MASTER_UI=http://spark-master:8080 	  -e SPARK_DRIVER_HOST=time-travel -e HDFS_URI=hdfs://namenode:8020 	  -e WEBHDFS_URL=http://namenode:9870 	  -e CORES_MAX=2 -e EXECUTOR_MEMORY=1g -e PYTHONUNBUFFERED=1 	  seisme-spark:latest python3 -m src.gold.time_travel
