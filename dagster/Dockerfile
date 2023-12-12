FROM python:3.11-bullseye

# Standard Python flags
ENV PYTHONUNBUFFERED 1
ENV PYTHONDONTWRITEBYTECODE 1
ARG POETRY_VERSION=1.6.1

# Spark flags
ENV SPARK_HOME /opt/spark

# Move to a temporary directory
WORKDIR /tmp

# Update packages & install JDK, Spark, Hadoop, Scala
RUN apt-get update && \
    apt-get install -y curl wget openjdk-11-jdk-headless libgdal-dev libgeos-dev && \
    wget https://dlcdn.apache.org/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz && \
    tar --extract --gzip --file spark-3.5.0-bin-hadoop3.tgz && \
    mkdir --parents "${SPARK_HOME}" && \
    mv spark-3.5.0-bin-hadoop3/* "${SPARK_HOME}" && \
    curl -fL https://github.com/coursier/coursier/releases/latest/download/cs-x86_64-pc-linux.gz | gzip -d > cs && \
    chmod +x cs && \
    ./cs setup --env --yes && \
    ./cs install scala:2.12.18 && \
    ./cs install scalac:2.12.18 && \
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

# Install poetry,
# set to not create an additional virtualenv (we are already in a container),
# enable parallel installs
RUN pip install "poetry==$POETRY_VERSION" && \
    poetry config virtualenvs.create false && \
    poetry config installer.max-workers 4 && \
    poetry config installer.no-binary shapely,fiona

# Copy package manifests inside the container
COPY pyproject.toml poetry.lock ./

# Install packages based on package manifests
# This is an automated process: do not ask for user input
RUN poetry install --no-interaction --no-ansi --with dagster,pipelines,spark,notebook

# Move to the directory where we mount the source code (see: docker-compose.yml)
WORKDIR /app

# Run the development server and listen on the specified port
CMD [ "dagster", "dev", "-h", "0.0.0.0", "-p", "3002" ]
