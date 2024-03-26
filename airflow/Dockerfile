FROM apache/airflow:2.8.4 

# Run installation tasks as root
USER 0


# Offical UID for spark process
ARG airflow_uid=${AIRFLOW_UID}



# Install Java
RUN mkdir -p /usr/share/man/man1 /usr/share/man/man2
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-11-jre

# Prints installed java version, just for checking
RUN java --version

USER airflow
RUN python -m pip install --upgrade pip
RUN pip install pyspark==3.3.0
RUN pip install apache-airflow-providers-apache-spark==3.0.0
RUN pip install apache-airflow-providers-docker==3.6.0