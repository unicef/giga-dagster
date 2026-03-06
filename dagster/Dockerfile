FROM python:3.11-bullseye

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
    apt-get install -y curl wget openjdk-11-jdk-headless gdal-bin libgdal-dev libgeos-dev g++ && \
    wget https://saunigigashare.blob.core.windows.net/sparkinstaller/spark-3.5.0-bin-hadoop3.tgz && \
    tar --extract --gzip --file spark-3.5.0-bin-hadoop3.tgz && \
    mkdir --parents "${SPARK_HOME}" && \
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

# Install poetry and setuptools (required for building some packages)
# Set virtualenv to be in-project for easier volume mounting
# Enable parallel installs for faster dependency installation
RUN pip install --upgrade pip setuptools wheel && \
    pip install "poetry==$POETRY_VERSION" && \
    poetry config virtualenvs.create true && \
    poetry config virtualenvs.in-project true && \
    poetry config installer.max-workers 4

# Move to the directory where we mount the source code (see: docker-compose.yml)
WORKDIR /app

# Run the development server and listen on the specified port
CMD [ "./docker-entrypoint.dev.sh" ]
