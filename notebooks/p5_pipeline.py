"""Partie 5 - Pipeline de production Spark-Kafka (Silver / Gold)."""
from pedali_common import CHECKPOINTS, GOLD, KAFKA_BOOTSTRAP, PKG_DELTA, PKG_KAFKA, SILVER, TOPIC, get_spark

spark = get_spark("pedali-p5", [PKG_KAFKA, PKG_DELTA])
# NB Delta : ajouter spark.sql.extensions et spark.sql.catalog.spark_catalog

# TODO 1. dropDuplicates(["trip_id", "status"]) + watermark
# TODO 2. Jointure stream-static avec stations (quartier, type de zone) + colonne trip_band
# TODO 3. Sink Silver Parquet : lake/silver/trips_clean/, partitionné par city et date
# TODO 4. Fenêtres par ville : tumbling 10 min, sliding 10/5 min, session_window 5 min par user_id
# TODO 5. Sink Gold Delta : lake/gold/kpi_city_10min/ + relecture batch + DESCRIBE HISTORY
# TODO 6. Arrêt / redémarrage : offsets du checkpoint vs kafka-consumer-groups.sh --describe

# Rapport : exactly-once (source rejouable, checkpoint, sink idempotent) et limite du sink console
