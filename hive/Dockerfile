FROM apache/hive:4.0.0

USER root

RUN apt-get update && \
    apt-get install -y curl wget && \
    apt-get clean

WORKDIR /opt/hive/lib

RUN wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-azure/3.3.4/hadoop-azure-3.3.4.jar \
    https://repo1.maven.org/maven2/com/microsoft/azure/azure-storage/8.6.6/azure-storage-8.6.6.jar \
    https://repo1.maven.org/maven2/com/azure/azure-storage-blob/12.24.0/azure-storage-blob-12.24.0.jar \
    https://repo1.maven.org/maven2/org/postgresql/postgresql/42.2.27/postgresql-42.2.27.jar

WORKDIR /opt/hive

ENTRYPOINT [ "/opt/hive/bin/hms-entrypoint.sh" ]
