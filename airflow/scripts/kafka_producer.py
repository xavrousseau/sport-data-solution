# scripts/kafka_producer.py

import os
import json
from kafka import KafkaProducer
from loguru import logger

# Lecture de la configuration depuis les variables d’environnement
KAFKA_BROKER_URL = os.getenv("KAFKA_BROKER_URL", "sport-redpanda:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_ACTIVITES", "sportdata_activites")

# Initialisation du producteur Kafka global (UTF-8, JSON, sans ACK fort pour perf)
try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER_URL,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=10,
        retries=3
    )
    logger.success(f"🔌 Producteur Kafka connecté → {KAFKA_BROKER_URL}")
except Exception as e:
    logger.error(f"❌ Erreur de connexion à Kafka : {e}")
    producer = None


def publier_evenement_kafka(message: dict, topic: str = None) -> None:
    """
    Publie un message JSON dans le topic Kafka spécifié.

    Args:
        message (dict): Le message à envoyer (sera converti en JSON).
        topic (str, optional): Nom du topic Kafka. Par défaut = variable KAFKA_TOPIC.
    """
    if not producer:
        logger.warning("⚠️ Kafka non disponible — message ignoré")
        return

    try:
        topic_final = topic or KAFKA_TOPIC
        producer.send(topic_final, value=message)
        producer.flush()
        logger.debug(f"📨 Message Kafka envoyé sur '{topic_final}' : {message}")
    except Exception as e:
        logger.error(f"❌ Erreur d’envoi Kafka : {e} — Message : {message}")
