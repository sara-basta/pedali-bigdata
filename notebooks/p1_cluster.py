"""Partie 1 - Mise en place du cluster et positionnement.

Lancement :
    docker exec -it jupyter spark-submit notebooks/p1_cluster.py
"""
import time

from pedali_common import get_spark

EXPECTED_EXECUTORS = 2      # un executor par Worker
TIMEOUT_S = 60


def wait_for_executors(sc, expected, timeout_s):
    """Attend que les executors soient enregistrés auprès du driver.

    Juste après getOrCreate(), les executors sont encore en train de démarrer sur
    les Workers : defaultParallelism ne compterait alors que les cœurs déjà
    disponibles (ou la valeur minimale 2).
    """
    tracker = sc._jsc.sc().statusTracker()
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        # getExecutorInfos() inclut le driver, d'où le -1
        if len(tracker.getExecutorInfos()) - 1 >= expected:
            return
        time.sleep(1)
    raise TimeoutError(f"Moins de {expected} executors enregistrés après {timeout_s} s")


def list_executors(sc):
    """Retourne les adresses host:port des executors (sans le driver)."""
    infos = sc._jsc.sc().statusTracker().getExecutorInfos()
    driver = sc.getConf().get("spark.driver.host")
    return sorted(f"{i.host()}:{i.port()}" for i in infos if i.host() != driver)


spark = get_spark("pedali-p1-cluster")
sc = spark.sparkContext
sc.setLogLevel("WARN")

wait_for_executors(sc, EXPECTED_EXECUTORS, TIMEOUT_S)

print("=" * 50)
print(f"Version Spark      : {spark.version}")
print(f"Master             : {sc.master}")
print(f"appName            : {sc.appName}")
print(f"defaultParallelism : {sc.defaultParallelism}")
print(f"Executors          : {', '.join(list_executors(sc))}")
print("=" * 50)

spark.stop()
