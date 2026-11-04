"""Partie 2 - Traitement batch avec SparkCore (RDD).

Prérequis : producer.py --mode file --n 50000, puis copier trips_history.jsonl
dans lake/bronze/history/
"""
from pedali_common import BRONZE, get_spark

spark = get_spark("pedali-p2")
sc = spark.sparkContext

# TODO 1. sc.textFile() + json.loads
# TODO 2. filter : trajets ENDED
# TODO 3. map + reduceByKey : chiffre d'affaires par ville
# TODO 4. Durée moyenne par ville et type de véhicule (un seul passage)
# TODO 5. Top 5 des stations de départ (takeOrdered ou sortBy + take)
# TODO 6. toDebugString() : transformations (lazy) vs actions

# Rapport : DAG Spark UI, nombre de stages, shuffle de reduceByKey, lineage et tolérance aux pannes

spark.stop()
