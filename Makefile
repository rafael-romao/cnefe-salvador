#!/usr/bin/make

SPARK_COMPOSE_FOLDER = ./docker-compose.yml
AIRFLOW_COMPOSE_FOLDER = ./airflow-docker-compose.yml


preprare_enviroment:
	@echo -e "AIRFLOW_UID=$(shell id -u)" >> ./.env


build:
	@make build_spark
	@make build_airflow

build_spark:
	@docker compose -f ${SPARK_COMPOSE_FOLDER} pull
	@docker compose -f ${SPARK_COMPOSE_FOLDER} build

build_airflow:
	@docker compose -f ${AIRFLOW_COMPOSE_FOLDER} pull
	@docker compose -f ${AIRFLOW_COMPOSE_FOLDER} build



up:
	@make up_spark
	@make up_airflow

up_spark:
	@docker compose -f ${SPARK_COMPOSE_FOLDER} up -d

up_airflow:
	@docker compose -f ${AIRFLOW_COMPOSE_FOLDER} up -d


		
stop:
	@make stop_airflow
	@make stop_spark

stop_spark:
	@docker compose -f ${SPARK_COMPOSE_FOLDER} stop

stop_airflow:
	@docker compose -f ${AIRFLOW_COMPOSE_FOLDER} stop


prune:
	@docker compose -f ${SPARK_COMPOSE_FOLDER} down --volumes --rmi all --remove-orphans
	@docker compose -f ${AIRFLOW_COMPOSE_FOLDER} down --volumes --rmi all --remove-orphans


tty_spark:
	@docker exec -it $(shell docker container ls -q --filter name=docker-spark-master) /bin/bash

tty_airflow:
	@docker exec -it $(shell docker container ls -q --filter name=cnefe_salvador-airflow-scheduler) /bin/bash

