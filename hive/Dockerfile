FROM openjdk:16-slim

ARG HADOOP_VERSION=3.3.0
ARG HIVE_METASTORE_VERSION=3.0.0
ARG POSTGRES_CONNECTOR_VERSION=42.6.0

ENV HADOOP_HOME="/opt/hadoop"
ENV PATH="/opt/spark/bin:/opt/hadoop/bin:${PATH}"
ENV DATABASE_DRIVER=org.postgresql.Driver
ENV DATABASE_TYPE=postgres
ENV DATABASE_TYPE_JDBC=postgresql
ENV DATABASE_PORT=5432

WORKDIR /app

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

RUN \
  echo "Install OS dependencies" && \
    build_deps="curl wget" && \
    apt-get update -y && \
    apt-get install -y $build_deps net-tools --no-install-recommends && \
  echo "Download and extract the Hadoop binary package" && \
    curl "https://archive.apache.org/dist/hadoop/core/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz" \
    | tar xvz -C /opt/ && \
    ln -s "/opt/hadoop-$HADOOP_VERSION" /opt/hadoop && \
    rm -r /opt/hadoop/share/doc

WORKDIR /opt/hadoop/share/hadoop/common/lib

RUN wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-azure/3.3.4/hadoop-azure-3.3.4.jar \
      https://repo1.maven.org/maven2/com/microsoft/azure/azure-storage/8.6.6/azure-storage-8.6.6.jar \
      https://repo1.maven.org/maven2/com/azure/azure-storage-blob/12.24.0/azure-storage-blob-12.24.0.jar

WORKDIR /app

RUN \
  echo "Download and install the standalone metastore binary" && \
    curl "https://downloads.apache.org/hive/hive-standalone-metastore-$HIVE_METASTORE_VERSION/hive-standalone-metastore-$HIVE_METASTORE_VERSION-bin.tar.gz" \
    | tar xvz -C /opt/ && \
    ln -s "/opt/apache-hive-metastore-$HIVE_METASTORE_VERSION-bin" /opt/hive-metastore && \
  echo "Fix 'java.lang.NoSuchMethodError: com.google.common.base.Preconditions.checkArgument'" && \
  echo "Keep this until this lands: https://issues.apache.org/jira/browse/HIVE-22915" && \
    rm "/opt/apache-hive-metastore-$HIVE_METASTORE_VERSION-bin/lib/guava-19.0.jar" && \
    cp "/opt/hadoop-$HADOOP_VERSION/share/hadoop/hdfs/lib/guava-27.0-jre.jar" "/opt/apache-hive-metastore-$HIVE_METASTORE_VERSION-bin/lib/" && \
  echo "Download and install the database connector" && \
    curl -L "https://jdbc.postgresql.org/download/postgresql-$POSTGRES_CONNECTOR_VERSION.jar" --output "/opt/postgresql-$POSTGRES_CONNECTOR_VERSION.jar" && \
    ln -s "/opt/postgresql-$POSTGRES_CONNECTOR_VERSION.jar" /opt/hadoop/share/hadoop/common/lib/ && \
    ln -s "/opt/postgresql-$POSTGRES_CONNECTOR_VERSION.jar" /opt/hive-metastore/lib/ && \
  echo "Purge build artifacts" && \
    apt-get purge -y --auto-remove $build_deps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

CMD [ "/opt/hive-metastore/bin/hms-entrypoint.sh" ]
