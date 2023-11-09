FROM bitnami/spark:3.5.0

USER root

RUN apt-get update && \
    apt-get install -y wget && \
    apt-get clean

ARG DELTA_SPARK_VERSION="3.0.0"
ARG DELTALAKE_VERSION="0.12.0"
ARG PANDAS_VERSION="1.5.3"
ARG ROAPI_VERSION="0.9.0"
ARG PYARROW_VERSION="11.0.0"

RUN pip install --no-cache-dir \
    delta-spark==${DELTA_SPARK_VERSION} \
    pandas==${PANDAS_VERSION} \
    deltalake==${DELTALAKE_VERSION} \
    pandas==${PANDAS_VERSION} \
    roapi==${ROAPI_VERSION} \
    pyarrow==${PYARROW_VERSION}

WORKDIR /opt/bitnami/spark/jars

RUN wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-azure/3.3.4/hadoop-azure-3.3.4.jar
RUN wget https://repo1.maven.org/maven2/com/microsoft/azure/azure-storage/8.6.6/azure-storage-8.6.6.jar
RUN wget https://repo1.maven.org/maven2/com/azure/azure-storage-blob/12.24.0/azure-storage-blob-12.24.0.jar
RUN wget https://repo1.maven.org/maven2/org/eclipse/jetty/jetty-util/9.4.51.v20230217/jetty-util-9.4.51.v20230217.jar
RUN wget https://repo1.maven.org/maven2/org/eclipse/jetty/jetty-util-ajax/11.0.14/jetty-util-ajax-11.0.14.jar
RUN wget https://repo1.maven.org/maven2/org/apache/spark/spark-avro_2.13/3.4.0/spark-avro_2.13-3.4.0.jar

USER 1001

WORKDIR /opt/bitnami/spark
