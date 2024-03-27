FROM apache/airflow:2.7.3 

# Run installation tasks as root
USER 0


RUN mkdir -p /usr/share/man/man1 /usr/share/man/man2
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-11-jre


RUN apt-get install -y iputils-ping

USER airflow

COPY ./requirements.txt /

RUN python -m pip install --upgrade pip
RUN pip install --trusted-host pypi.python.org --no-cache-dir --user -r /requirements.txt
