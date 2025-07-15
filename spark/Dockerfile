# ============================================================================
# Fichier : spark/Dockerfile
# Objet   : Image Spark custom avec support Kafka
# ============================================================================
FROM bitnami/spark:3.5.1

USER root

# ➕ Installer curl (via apt)
RUN install_packages curl

# 📥 Téléchargement manuel du connecteur Spark-Kafka 0.10 + Kafka client
RUN mkdir -p /opt/bitnami/spark/jars && \
    curl -L -o /opt/bitnami/spark/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar \
        https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar && \
    curl -L -o /opt/bitnami/spark/jars/kafka-clients-3.5.1.jar \
        https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar && \
    curl -L -o /opt/bitnami/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.1.jar \
        https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar

USER 1001