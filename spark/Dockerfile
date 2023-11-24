FROM python:3.11-slim as deps

ARG POETRY_VERSION=1.6.1

RUN pip install "poetry==$POETRY_VERSION"

WORKDIR /tmp

COPY dagster/pyproject.toml dagster/poetry.lock ./

RUN poetry export --without-hashes --with pipelines,spark -f requirements.txt > requirements.txt

FROM bitnami/spark:3.5.0 as base

USER root

RUN apt-get update && \
    apt-get install -y wget && \
    apt-get clean

COPY --from=deps /tmp/requirements.txt /tmp/requirements.txt

RUN pip install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /opt/bitnami/spark/jars

RUN wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-azure/3.3.4/hadoop-azure-3.3.4.jar
RUN wget https://repo1.maven.org/maven2/com/microsoft/azure/azure-storage/8.6.6/azure-storage-8.6.6.jar
RUN wget https://repo1.maven.org/maven2/com/azure/azure-storage-blob/12.24.0/azure-storage-blob-12.24.0.jar
RUN wget https://repo1.maven.org/maven2/org/eclipse/jetty/jetty-util/9.4.51.v20230217/jetty-util-9.4.51.v20230217.jar
RUN wget https://repo1.maven.org/maven2/org/eclipse/jetty/jetty-util-ajax/11.0.14/jetty-util-ajax-11.0.14.jar
RUN wget https://repo1.maven.org/maven2/org/apache/spark/spark-avro_2.13/3.4.0/spark-avro_2.13-3.4.0.jar

USER 1001

WORKDIR /opt/bitnami/spark

FROM base as master

RUN /opt/bitnami/spark/sbin/start-thriftserver.sh \
    --hiveconf hive.server2.thrift.port=$THRIFT_PORT \
    --master "spark://localhost:$SPARK_MASTER_PORT"
