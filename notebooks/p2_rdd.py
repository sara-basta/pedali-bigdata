"""Partie 2 - Traitement batch avec SparkCore (RDD).

Prérequis :
    docker exec -it jupyter python producer/gen_fake_history.py --n 50000
    (ou producer.py --mode file --n 50000), puis copier trips_history.jsonl dans
    lake/bronze/history/ : docker exec -it jupyter python producer/upload_to_minio.py

Lancement (python et non spark-submit : spark.jars.packages, défini dans get_spark,
n'est pris en compte que si c'est PySpark qui démarre la JVM ; sinon S3A est introuvable) :
    docker exec -it jupyter python notebooks/p2_rdd.py
"""
import json
import time

from pedali_common import BRONZE, get_spark

HISTORY = f"{BRONZE}/history/trips_history.jsonl"
DEDUP = True        # ~2 % de doublons dans le générateur (même trip_id + même status)

spark = get_spark("pedali-p2-rdd")
sc = spark.sparkContext
sc.setLogLevel("WARN")


def timed(label, fn):
    """Exécute une action et affiche son temps (pour la comparaison RDD vs DataFrame)."""
    t0 = time.time()
    result = fn()
    print(f"[{time.time() - t0:6.2f} s] {label}")
    return result


def parse(line):
    """Une ligne JSON -> dict ; les lignes invalides sont ignorées."""
    try:
        return [json.loads(line)]
    except ValueError:
        return []


# 1. Chargement + parsing (transformations lazy)
raw = sc.textFile(HISTORY)
trips = raw.flatMap(parse)

# 2. Trajets terminés uniquement
ended = trips.filter(lambda t: t.get("status") == "ENDED")
if DEDUP:
    # un trajet ENDED = un seul événement : on garde un exemplaire par trip_id
    ended = (ended.map(lambda t: (t["trip_id"], t))
                  .reduceByKey(lambda a, b: a)
                  .values())
ended.cache()

print("=" * 60)
print(f"Lignes lues         : {timed('count(raw)', raw.count)}")
print(f"Trajets ENDED       : {timed('count(ended)', ended.count)}")

# 3. Chiffre d'affaires total par ville
revenue = (ended.map(lambda t: (t["city"], t["price_mad"] or 0.0))
                .reduceByKey(lambda a, b: a + b))
revenue_rows = timed("CA par ville", lambda: sorted(revenue.collect()))
print("\nChiffre d'affaires par ville (MAD)")
for city, ca in revenue_rows:
    print(f"  {city:<12} {ca:>12,.2f}")

# 4. Durée moyenne par (ville, type de véhicule) : somme et compte en un passage
avg_duration = (ended.map(lambda t: ((t["city"], t["vehicle_type"]), t["duration_s"] or 0))
                     .aggregateByKey((0, 0),
                                     lambda acc, d: (acc[0] + d, acc[1] + 1),
                                     lambda a, b: (a[0] + b[0], a[1] + b[1]))
                     .mapValues(lambda s: s[0] / s[1]))
avg_rows = timed("Durée moyenne par ville et véhicule", lambda: sorted(avg_duration.collect()))
print("\nDurée moyenne d'un trajet (secondes)")
for (city, vtype), avg in avg_rows:
    print(f"  {city:<12} {vtype:<8} {avg:>8.1f}")

# 5. Top 5 des stations de départ
top_stations = (ended.map(lambda t: (t["start_station"], 1))
                     .reduceByKey(lambda a, b: a + b))
top5 = timed("Top 5 stations", lambda: top_stations.takeOrdered(5, key=lambda kv: -kv[1]))
print("\nTop 5 des stations de départ")
for station, n in top5:
    print(f"  {station:<12} {n:>6} trajets")

# 6. Lineage
print("\nLineage (toDebugString) du RDD du chiffre d'affaires :")
print(revenue.toDebugString().decode("utf-8"))
print("""
Transformations (lazy) : flatMap, filter, map, reduceByKey, aggregateByKey, mapValues, cache
Actions (déclenchent un job) : count, collect, takeOrdered
""")

# Pour la capture du DAG : garder l'application ouverte, puis ouvrir http://localhost:4040
input("Spark UI sur http://localhost:4040 - capture du DAG, puis Entrée pour terminer...")
spark.stop()