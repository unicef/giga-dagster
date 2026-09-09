FROM apache/hive:4.0.0

USER root

# bullseye-security's InRelease file has expired — Debian has stopped publishing fresh
# Release files for it as this suite ages past EOL, so apt refuses to trust it and
# `apt-get update` fails; bullseye/bullseye-updates alone satisfy these packages, so drop
# it. Revisit when bumping off this bullseye-based base image.
RUN sed -i '/security.debian.org/d' /etc/apt/sources.list && \
    apt-get update && \
    apt-get install -y curl wget default-jdk-headless && \
    apt-get clean

WORKDIR /opt/hive/lib

RUN wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-azure/3.3.6/hadoop-azure-3.3.6.jar \
    https://repo1.maven.org/maven2/com/microsoft/azure/azure-storage/8.6.6/azure-storage-8.6.6.jar \
    https://repo1.maven.org/maven2/com/azure/azure-storage-blob/12.21.1/azure-storage-blob-12.21.1.jar \
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
