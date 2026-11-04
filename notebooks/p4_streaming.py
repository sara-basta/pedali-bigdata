"""Partie 4 - Structured Streaming depuis Kafka.

Prérequis : producer.py --mode kafka --rate 20
"""
from pedali_common import BRONZE, CHECKPOINTS, KAFKA_BOOTSTRAP, PKG_KAFKA, TOPIC, get_spark

spark = get_spark("pedali-p4", [PKG_KAFKA])

# TODO 1. spark.readStream.format("kafka") sur le topic trips
# TODO 2. from_json sur value avec le schéma de la Partie 3
# TODO 3. Sink console, outputMode("append")
# TODO 4. Sink JSON brut dans lake/bronze/trips_raw/ avec checkpoint dédié
# TODO 5. Nombre de trajets par ville et statut, outputMode("complete")
# TODO 6. Watermark de 10 min sur event_time (exemple d'un événement en retard de 15 min)

# Rapport : source -> transformation -> sink, micro-batch vs continu, modes append/update/complete
