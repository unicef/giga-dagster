FROM bitnami/spark:3.5.0 as base

USER root

WORKDIR /tmp

RUN apt-get update && \
    apt-get install -y curl wget && \
    apt-get clean

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

WORKDIR /opt/bitnami/spark/jars

RUN wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-azure/3.3.4/hadoop-azure-3.3.4.jar
RUN wget https://repo1.maven.org/maven2/com/microsoft/azure/azure-storage/8.6.6/azure-storage-8.6.6.jar
RUN wget https://repo1.maven.org/maven2/com/azure/azure-storage-blob/12.24.0/azure-storage-blob-12.24.0.jar
RUN wget https://repo1.maven.org/maven2/org/eclipse/jetty/jetty-util/9.4.51.v20230217/jetty-util-9.4.51.v20230217.jar
RUN wget https://repo1.maven.org/maven2/org/eclipse/jetty/jetty-util-ajax/11.0.14/jetty-util-ajax-11.0.14.jar
RUN wget https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.0.0/delta-spark_2.12-3.0.0.jar
RUN wget https://repo1.maven.org/maven2/io/delta/delta-storage/3.0.0/delta-storage-3.0.0.jar

USER 1001

RUN curl -fL https://github.com/coursier/coursier/releases/latest/download/cs-x86_64-pc-linux.gz | gzip -d > cs && \
    chmod +x cs && \
    ./cs setup --env --yes && \
    ./cs install scala:2.12.18 && \
    ./cs install scalac:2.12.18

WORKDIR /opt/bitnami/spark

FROM base as master

RUN /opt/bitnami/spark/sbin/start-thriftserver.sh \
    --hiveconf hive.server2.thrift.port=$THRIFT_PORT \
    --master "spark://localhost:$SPARK_MASTER_PORT"
