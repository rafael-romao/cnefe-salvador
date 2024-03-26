FROM bitnami/spark:3.4

# Run installation tasks as root
# USER 0



# WORKDIR /opt/bitnami/spark/

# Offical UID for spark process
# ARG spark_uid=185

# Specify the user info for spark_uid
# RUN useradd -d /home/sparkuser -ms /bin/bash -u ${spark_uid} sparkuser \
#     && chown -R sparkuser /opt/bitnami/spark/

# ENV PYSPARK_MAJOR_PYTHON_VERSION=3 \
#     APP_DIR=/opt/bitnami/spark

# USER ${spark_uid}

# Python script to start the program
# COPY --chown=sparkuser ./app/src/ ${APP_DIR}
# COPY ./app/src/ ${APP_DIR}


# Ensure owned by Spark
# RUN chown -R sparkuser:sparkuser ${APP_DIR}/*



# EXPOSE 4040

# RUN $SPARK_HOME/bin/spark-submit --conf spark.jars.ivy=/opt/bitnami/spark/ivy ./app/landing_to_raw.py ./data/landing/29.txt ./data/raw/cnefe_bahia/
# RUN $SPARK_HOME/bin/spark-submit --conf spark.jars.ivy=/opt/bitnami/spark/ivy ./app/raw_to_base_a.py ./data/raw/cnefe_bahia/ ./data/cleaned/base_a/
