"""Partie 1 - Mise en place du cluster et positionnement."""
from pedali_common import get_spark

spark = get_spark("pedali-p1")
sc = spark.sparkContext

# TODO 1. Afficher : version de Spark, master, appName, sc.defaultParallelism

# Rapport (1 page) :
# TODO - Rôle du Driver, des Executors, du Cluster Manager et du SparkContext
# TODO - Deux limites de MapReduce/Hadoop corrigées par Spark, reliées au projet
# Captures : Spark Master UI (2 Workers actifs), console MinIO (bucket lake)

spark.stop()
