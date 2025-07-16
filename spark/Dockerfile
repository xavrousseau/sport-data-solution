# ============================================================================
# Fichier : spark/Dockerfile
# Objet   : Image Spark custom avec support Kafka + librairies Python utiles
# ============================================================================

FROM bitnami/spark:3.5.1

USER root

# ➕ Installer curl (via apt)
RUN install_packages curl

# Dépendances Python centralisées
COPY ./spark/requirements_spark.txt /tmp/requirements_spark.txt
RUN pip install -r /tmp/requirements_spark.txt

# 📥 Télécharger manuellement les connecteurs Spark-Kafka (version 3.5.1)
RUN mkdir -p /opt/bitnami/spark/jars && \
    curl -L -o /opt/bitnami/spark/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar \
        https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar && \
    curl -L -o /opt/bitnami/spark/jars/kafka-clients-3.5.1.jar \
        https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar && \
    curl -L -o /opt/bitnami/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.1.jar \
        https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar

USER 1001
