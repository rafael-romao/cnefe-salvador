FROM bitnami/spark:3.4

# Run installation tasks as root
USER 0

RUN  apt-get update \
  && apt-get install -y wget

USER 1001

RUN wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk/1.12.353/aws-java-sdk-1.12.353.jar -P /opt/bitnami/spark/jars 
RUN wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-s3/1.12.353/aws-java-sdk-s3-1.12.353.jar -P /opt/bitnami/spark/jars 
RUN wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.353/aws-java-sdk-bundle-1.12.353.jar -P /opt/bitnami/spark/jars

RUN pip install minio
RUN pip install pandas

EXPOSE 4040







# RUN $SPARK_HOME/bin/spark-submit --conf spark.jars.ivy=/opt/bitnami/spark/ivy ./app/landing_to_raw.py ./data/landing/29.txt ./data/raw/cnefe_bahia/
# RUN $SPARK_HOME/bin/spark-submit --conf spark.jars.ivy=/opt/bitnami/spark/ivy ./app/base_a.py ./data/raw/cnefe_bahia/ ./data/cleaned/base_a/
