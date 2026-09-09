FROM apache/hive:4.0.0

USER root

# bullseye is EOL: deb.debian.org's live bullseye-security index and pool have drifted out of
# sync, causing spurious 404s, and its InRelease file goes stale as this suite ages past EOL.
# Pin security to a fixed snapshot.debian.org timestamp for an index+pool that are guaranteed
# consistent with each other, and skip the validity check since a pinned snapshot is always
# "expired" by design. Revisit when bumping off this bullseye-based base image.
RUN sed -i 's|^deb http://[a-z.]*/debian-security|deb http://snapshot.debian.org/archive/debian-security/20250721T000000Z|' /etc/apt/sources.list && \
    apt-get update -o Acquire::Check-Valid-Until=false && \
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
