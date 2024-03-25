FROM bitnami/spark:3.4

# Run installation tasks as root
USER 0



WORKDIR /opt/bitnami/spark/

# Offical UID for spark process
ARG spark_uid=185

# Specify the user info for spark_uid
RUN useradd -d /home/sparkuser -ms /bin/bash -u ${spark_uid} sparkuser \
    && chown -R sparkuser /opt/bitnami/spark/
    # && chown -R sparkuser /data


ENV PYSPARK_MAJOR_PYTHON_VERSION=3 \
    APP_DIR=/opt/bitnami/spark

USER ${spark_uid}

# Python script to start the program
COPY --chown=sparkuser ./app/src/landing_to_raw.py ${APP_DIR}
# COPY --chown=sparkuser ./data ${APP_DIR}/data


# Ensure owned by Spark
RUN chown -R sparkuser:sparkuser ${APP_DIR}/*


EXPOSE 4040

# RUN chmod 777 /opt/bitnami/spark/app/

# RUN $SPARK_HOME/bin/spark-submit --conf spark.jars.ivy=/opt/bitnami/spark/ivy ./landing_to_raw.py ./data/landing/29.txt ./data/cleaned/cnefe/

# RUN $SPARK_HOME/bin/spark-submit --conf spark.jars.ivy=/opt/spark/ivy  ./app/test.py