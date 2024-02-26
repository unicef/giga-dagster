FROM python:3.11-bullseye AS base

# Standard Python flags
ENV PYTHONUNBUFFERED 1
ENV PYTHONDONTWRITEBYTECODE 1
ARG POETRY_VERSION=1.6.1

# Spark flags
ENV SPARK_HOME /opt/spark

# Move to a temporary directory
WORKDIR /tmp

# Update packages & install JDK, Spark, Hadoop
RUN apt-get update && \
    apt-get install -y curl wget openjdk-11-jdk-headless libgdal-dev libgeos-dev && \
    wget https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz && \
    tar --extract --gzip --file spark-3.5.0-bin-hadoop3.tgz && \
    mkdir --parents /opt/spark && \
    mv spark-3.5.0-bin-hadoop3/* "${SPARK_HOME}" && \
    apt-get clean

# Move to Spark JARs directory and download additional dependencies
WORKDIR /opt/spark/jars

RUN wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-azure/3.3.4/hadoop-azure-3.3.4.jar \
    https://repo1.maven.org/maven2/com/microsoft/azure/azure-storage/8.6.6/azure-storage-8.6.6.jar \
    https://repo1.maven.org/maven2/com/azure/azure-storage-blob/12.24.0/azure-storage-blob-12.24.0.jar \
    https://repo1.maven.org/maven2/org/eclipse/jetty/jetty-util/9.4.51.v20230217/jetty-util-9.4.51.v20230217.jar \
    https://repo1.maven.org/maven2/org/eclipse/jetty/jetty-util-ajax/11.0.14/jetty-util-ajax-11.0.14.jar \
    https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.0.0/delta-spark_2.12-3.0.0.jar \
    https://repo1.maven.org/maven2/io/delta/delta-storage/3.0.0/delta-storage-3.0.0.jar

FROM base AS deps

# Install Poetry and don't create a virtualenv (we are already in a container)
RUN pip install "poetry==$POETRY_VERSION" && \
    poetry config virtualenvs.create false

WORKDIR /tmp

# Copy package manifests
COPY pyproject.toml poetry.lock ./

# Convert package manifests to requirements.txt format and exclude dev dependencies
RUN poetry export -f requirements.txt --without-hashes --with dagster,pipelines,spark > requirements.txt

FROM base AS prod

# Copy the generated file from the previous stage
COPY --from=deps /tmp/requirements.txt .

# Install packages defined in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

WORKDIR /app

# Copy source code into the container
COPY src ./src
COPY scripts ./scripts
COPY schemas ./schemas
COPY sql ./sql

# Read the PORT environment variable, otherwise default to the specified port
ENV PORT 3002

CMD [ "/bin/sh", "-c", "dagster-webserver -h 0.0.0.0 -p $PORT" ]
