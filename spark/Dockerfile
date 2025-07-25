# ============================================================================
# Fichier : spark/Dockerfile
# Objet   : Image Spark custom avec support Kafka + Delta Lake + MinIO + Pandas
# Auteur  : Xavier Rousseau | Juillet 2025
# ============================================================================

FROM bitnami/spark:3.5.1

USER root

# ➕ Installer curl (via apt)
RUN install_packages curl

# 📦 Dépendances Python (loguru, pandas, minio, etc.)
COPY ./spark/requirements_spark.txt /tmp/requirements_spark.txt
RUN pip install --no-cache-dir -r /tmp/requirements_spark.txt

# 📥 Connecteurs Kafka pour Spark
RUN mkdir -p /opt/bitnami/spark/jars && \
    curl -L -o /opt/bitnami/spark/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar \
        https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar && \
    curl -L -o /opt/bitnami/spark/jars/kafka-clients-3.5.1.jar \
        https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar && \
    curl -L -o /opt/bitnami/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.1.jar \
        https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar

# 📥 Connecteurs Delta Lake + dépendances Hadoop + S3A
RUN curl -L -o /opt/bitnami/spark/jars/delta-core_2.12-3.1.0.jar \
        https://repo1.maven.org/maven2/io/delta/delta-core_2.12/3.1.0/delta-core_2.12-3.1.0.jar && \
    curl -L -o /opt/bitnami/spark/jars/delta-storage-3.1.0.jar \
        https://repo1.maven.org/maven2/io/delta/delta-storage/3.1.0/delta-storage-3.1.0.jar && \
    curl -L -o /opt/bitnami/spark/jars/delta-spark_2.12-3.1.0.jar \
        https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.1.0/delta-spark_2.12-3.1.0.jar && \
    curl -L -o /opt/bitnami/spark/jars/hadoop-aws-3.3.6.jar \
        https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.6/hadoop-aws-3.3.6.jar && \
    curl -L -o /opt/bitnami/spark/jars/aws-java-sdk-bundle-1.11.1026.jar \
        https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.1026/aws-java-sdk-bundle-1.11.1026.jar && \
    curl -L -o /opt/bitnami/spark/jars/hadoop-common-3.3.6.jar \
        https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.3.6/hadoop-common-3.3.6.jar && \
    curl -L -o /opt/bitnami/spark/jars/hadoop-auth-3.3.6.jar \
        https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-auth/3.3.6/hadoop-auth-3.3.6.jar && \
    curl -L -o /opt/bitnami/spark/jars/commons-configuration2-2.7.jar \
        https://repo1.maven.org/maven2/org/apache/commons/commons-configuration2/2.7/commons-configuration2-2.7.jar && \
    curl -L -o /opt/bitnami/spark/jars/commons-lang3-3.12.0.jar \
        https://repo1.maven.org/maven2/org/apache/commons/commons-lang3/3.12.0/commons-lang3-3.12.0.jar

USER 1001
