FROM python:3.11-slim AS deps

ARG POETRY_VERSION=1.6.1

RUN pip install "poetry==$POETRY_VERSION"

WORKDIR /tmp

COPY dagster/pyproject.toml dagster/poetry.lock ./

RUN poetry export --without-hashes --with pipelines,spark -f requirements.txt > requirements.txt

FROM public.ecr.aws/bitnami/spark:3.5.0-debian-11-r18

USER root

WORKDIR /tmp

RUN apt-get update && \
    apt-get install -y curl wget gdal-bin libgdal-dev libgeos-dev g++ && \
    apt-get clean

COPY --from=deps /tmp/requirements.txt /tmp/requirements.txt

RUN pip install --default-timeout=1000 --no-cache-dir -r requirements.txt

WORKDIR /opt/bitnami/spark/jars

RUN wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-azure/3.3.4/hadoop-azure-3.3.4.jar \
    https://repo1.maven.org/maven2/com/microsoft/azure/azure-storage/8.6.6/azure-storage-8.6.6.jar \
    https://repo1.maven.org/maven2/com/azure/azure-storage-blob/12.24.0/azure-storage-blob-12.24.0.jar \
    https://repo1.maven.org/maven2/org/eclipse/jetty/jetty-util/9.4.51.v20230217/jetty-util-9.4.51.v20230217.jar \
    https://repo1.maven.org/maven2/org/eclipse/jetty/jetty-util-ajax/11.0.14/jetty-util-ajax-11.0.14.jar \
    https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.0.0/delta-spark_2.12-3.0.0.jar \
    https://repo1.maven.org/maven2/io/delta/delta-storage/3.0.0/delta-storage-3.0.0.jar

USER 1001

WORKDIR /opt/bitnami/spark
