#!/bin/sh
# Builds the docker image
# Run in the folder where Dockerfile is located, the project root
docker build -t cluster-apache-spark:3.4 .