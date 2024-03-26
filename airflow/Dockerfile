FROM apache/airflow:2.7.3 

# Run installation tasks as root
USER 0


RUN apt-get update


USER airflow
RUN python -m pip install --upgrade pip
COPY ./requirements.txt /
RUN pip install --no-cache-dir --user -r /requirements.txt

# RUN pip install --no-cache-dir pyspark==3.5.0
# RUN pip install --no-cache-dir apache-airflow-providers-apache-spark==4.3.0