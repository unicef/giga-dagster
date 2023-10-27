FROM bitnami/spark:3.5.0

USER root

RUN apt-get update && \
    apt-get install -y wget && \
    apt-get clean

WORKDIR /opt/bitnami/spark/jars

RUN wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-azure/3.3.6/hadoop-azure-3.3.6.jar && \
    wget https://repo1.maven.org/maven2/com/microsoft/azure/azure-storage/8.6.6/azure-storage-8.6.6.jar

RUN pip install pandas pyarrow

USER spark

WORKDIR /opt/bitnami/spark
