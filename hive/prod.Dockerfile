FROM apache/hive:4.0.0

USER root

RUN apt-get update && \
    apt-get install -y curl wget default-jdk-headless && \
    apt-get clean

WORKDIR /opt/hive/lib

RUN wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-azure/3.3.4/hadoop-azure-3.3.4.jar \
    https://repo1.maven.org/maven2/com/microsoft/azure/azure-storage/8.6.6/azure-storage-8.6.6.jar \
    https://repo1.maven.org/maven2/com/azure/azure-storage-blob/12.24.0/azure-storage-blob-12.24.0.jar \
    https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar

# Compile FixedSASTokenProvider (not included in the standard hadoop-azure JAR)
COPY FixedSASTokenProvider.java /tmp/sas/FixedSASTokenProvider.java
RUN mkdir -p /tmp/sas/src/org/apache/hadoop/fs/azurebfs/sas /tmp/sas/classes && \
    cp /tmp/sas/FixedSASTokenProvider.java /tmp/sas/src/org/apache/hadoop/fs/azurebfs/sas/ && \
    javac -source 8 -target 8 -cp "/opt/hive/lib/*:/opt/hadoop/share/hadoop/common/*:/opt/hadoop/share/hadoop/common/lib/*" \
        -d /tmp/sas/classes \
        /tmp/sas/src/org/apache/hadoop/fs/azurebfs/sas/FixedSASTokenProvider.java && \
    jar cf /opt/hive/lib/fixed-sas-token-provider.jar -C /tmp/sas/classes . && \
    rm -rf /tmp/sas

COPY ./hms-entrypoint.sh /opt/hive/bin/hms-entrypoint.sh
COPY ./metastore-site.template.xml /opt/hive/tpl/metastore-site.template.xml
COPY ./hive-site.template.xml /opt/hive/tpl/hive-site.template.xml

WORKDIR /opt/hive

ENTRYPOINT [ "/opt/hive/bin/hms-entrypoint.sh" ]
