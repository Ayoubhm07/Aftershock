FROM spark:3.5.6-python3

USER root

ARG MAVEN=https://repo1.maven.org/maven2
ARG SPARK_VERSION=3.5.6
ARG KAFKA_CLIENTS=3.4.1
ARG COMMONS_POOL=2.11.1
ARG DELTA_VERSION=3.2.1

RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends curl ca-certificates; \
    rm -rf /var/lib/apt/lists/*; \
    cd "${SPARK_HOME}/jars"; \
    curl -fsSLO "${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_2.12/${SPARK_VERSION}/spark-sql-kafka-0-10_2.12-${SPARK_VERSION}.jar"; \
    curl -fsSLO "${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_2.12/${SPARK_VERSION}/spark-token-provider-kafka-0-10_2.12-${SPARK_VERSION}.jar"; \
    curl -fsSLO "${MAVEN}/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS}/kafka-clients-${KAFKA_CLIENTS}.jar"; \
    curl -fsSLO "${MAVEN}/org/apache/commons/commons-pool2/${COMMONS_POOL}/commons-pool2-${COMMONS_POOL}.jar"; \
    curl -fsSLO "${MAVEN}/io/delta/delta-spark_2.12/${DELTA_VERSION}/delta-spark_2.12-${DELTA_VERSION}.jar"; \
    curl -fsSLO "${MAVEN}/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar"; \
    for j in spark-sql-kafka-0-10_2.12-${SPARK_VERSION}.jar kafka-clients-${KAFKA_CLIENTS}.jar \
             delta-spark_2.12-${DELTA_VERSION}.jar delta-storage-${DELTA_VERSION}.jar; do test -s "$j"; done

COPY docker/spark/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY conf/core-site.xml /opt/spark/conf/core-site.xml
COPY src /opt/app/src

ENV PYTHONPATH=/opt/app:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip \
    PYTHONUNBUFFERED=1 \
    HADOOP_CONF_DIR=/opt/spark/conf

RUN ln -sf /usr/bin/python3 /usr/local/bin/python

WORKDIR /opt/app
USER spark
