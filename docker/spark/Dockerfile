FROM bitnami/spark:3.5.0

# Run installation tasks as root
USER 0

RUN  apt-get update \
  && apt-get install -y wget

USER 1001

RUN wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.353/aws-java-sdk-bundle-1.12.353.jar -P /opt/bitnami/spark/jars

RUN pip install jellyfish

EXPOSE 4040