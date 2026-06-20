FROM debezium/connect:2.5

USER root

RUN microdnf update -y && \
  microdnf install -y curl unzip tar && \
  microdnf clean all

RUN mkdir -p /kafka/connect

ARG CONFLUENT_VERSION=7.6.0
ARG GCS_VERSION=3.2.0

RUN curl -fsSL \
  "https://packages.confluent.io/archive/7.6/confluent-community-${CONFLUENT_VERSION}.tar.gz" \
  -o /tmp/confluent-community.tar.gz && \
  tar -xzf /tmp/confluent-community.tar.gz -C /tmp/ && \
  mkdir -p /kafka/connect/kafka-connect-avro-converter && \
  cp /tmp/confluent-${CONFLUENT_VERSION}/share/java/kafka-serde-tools/*.jar \
  /kafka/connect/kafka-connect-avro-converter/ && \
  cp /tmp/confluent-${CONFLUENT_VERSION}/share/java/schema-registry/*.jar \
  /kafka/connect/kafka-connect-avro-converter/ 2>/dev/null || true && \
  rm -rf /tmp/confluent-community.tar.gz /tmp/confluent-${CONFLUENT_VERSION}

RUN curl -fsSL \
  "https://github.com/Aiven-Open/cloud-storage-connectors-for-apache-kafka/releases/download/v${GCS_VERSION}/gcs-sink-connector-for-apache-kafka-${GCS_VERSION}.zip" \
  -o /tmp/gcs-sink.zip && \
  unzip -q /tmp/gcs-sink.zip -d /kafka/connect/ && \
  rm -f /tmp/gcs-sink.zip

RUN echo "=== Installed plugins ===" && ls -1 /kafka/connect/ && \
  echo "=== Avro converter JARs ===" && \
  ls /kafka/connect/kafka-connect-avro-converter/ | grep -i avro

USER kafka