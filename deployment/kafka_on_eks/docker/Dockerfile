FROM quay.io/strimzi/kafka:0.47.0-kafka-3.9.0

USER root:root

# Install curl and tar to download the Aiven plugin
# RUN microdnf install -y curl tar gzip && microdnf clean all

# Add Aiven S3 tiered storage plugin (v1.1.1)
RUN mkdir -p /opt/kafka/tiered-storage-s3
RUN curl -sL https://github.com/Aiven-Open/tiered-storage-for-apache-kafka/releases/download/v1.1.1/core-1.1.1.tgz | tar -xz --strip-components=1 -C /opt/kafka/tiered-storage-s3
RUN curl -sL https://github.com/Aiven-Open/tiered-storage-for-apache-kafka/releases/download/v1.1.1/s3-1.1.1.tgz | tar -xz --strip-components=1 -C /opt/kafka/tiered-storage-s3

USER 1001
