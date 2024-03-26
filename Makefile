#!/usr/bin/make


preprare_enviroment:
	@echo -e "AIRFLOW_UID=$(shell id -u)" >> ./.env
	@echo -e "AIRFLOW_GID=$(shell id -g)" >> ./.env


build:
	@docker compose -f ./spark/docker-compose.yml pull
	@docker compose -f ./spark/docker-compose.yml build
	@docker compose -f ./airflow/airflow-docker-compose.yml pull
	@docker compose -f ./airflow/airflow-docker-compose.yml build


build_spark:
	@docker compose -f ./spark/docker-compose.yml pull
	@docker compose -f ./spark/docker-compose.yml build

build_airflow:
	@docker compose -f ./airflow/airflow-docker-compose.yml pull
	@docker compose -f ./airflow/airflow-docker-compose.yml build


up_spark:
	@docker compose -f ./spark/docker-compose.yml up -d

up_airflow:
	@echo "Initiating Airflow"
	@docker compose -f ./airflow/airflow-docker-compose.yml up -d

up_services:
	@echo "Initiating Spark and Airflow" 
	@make up_spark
	@make up_airflow
		
stop:
	@docker compose -f ./spark/docker-compose.yml stop
	@docker compose -f ./airflow/airflow-docker-compose.yml stop

stop_airflow:
	@docker compose -f ./airflow/airflow-docker-compose.yml stop

# run: ## Start the Spark service
# 	@docker run -u $(id -u):$(id -g)  -it -v ./data:/opt/bitnami/spark/data /spark/spark-docker-compose.yml bash

prune:
	@docker compose -f ./spark/docker-compose.yml down --volumes --rmi all --remove-orphans
	@docker compose -f ./airflow/airflow-docker-compose.yml down --volumes --rmi all --remove-orphans

tty:
	@docker exec -it $(shell docker container ls -q --filter ancestor=spark-spark-master) /bin/bash

airflow_tty:
	@docker exec -it $(shell docker container ls -q --filter ancestor= airflow-airflow-scheduler-1) /bin/bash