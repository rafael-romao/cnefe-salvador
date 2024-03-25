#!/usr/bin/make

run: ## Start the Spark service
	@docker run -u $(id -u):$(id -g)  -it -v ./data:/opt/bitnami/spark/data_test cluster-apache-spark:3.4 bash

build: ## Stop the Spark service
	@docker build -t cluster-apache-spark:3.4 .

prune:
	@docker system prune -a --volumes
# restart: stop start ## Restart the Spark service

# tty:
# 	 docker exec -it $(shell docker container ls -q --filter ancestor=docker-spark-master) /bin/bash