"""Partie 3 - Spark SQL et DataFrames.

Prérequis : trips_history.jsonl dans lake/bronze/history/ (Partie 2) et data/stations.json.

Lancement :
    docker exec -it jupyter python notebooks/p3_dataframes.py
"""
import os
import time

from pyspark.sql import functions as F
from pyspark.sql.types import (DoubleType, IntegerType, LongType, StringType,
                               StructField, StructType)

from pedali_common import BRONZE, SILVER, get_spark

HISTORY = f"{BRONZE}/history/trips_history.jsonl"
# stations.json est dans le dossier du projet, monté sur le driver ET sur les workers
STATIONS = os.getenv("STATIONS_PATH", "file:///opt/pedali/data/stations.json")
SILVER_OUT = f"{SILVER}/trips_history"
PLAN_FILE = os.getenv("PLAN_FILE", "/opt/pedali/data/p3_explain.txt")

spark = get_spark("pedali-p3-dataframes")
sc = spark.sparkContext
sc.setLogLevel("WARN")

timings = {}


def timed(label, fn):
    """Exécute une action, mémorise et affiche son temps (comparaison RDD vs DataFrame)."""
    t0 = time.time()
    result = fn()
    timings[label] = time.time() - t0
    print(f"[{timings[label]:6.2f} s] {label}")
    return result


# ---------------------------------------------------------------- 1. Schémas explicites
# Ce schéma est réutilisé en Partie 4 (from_json). event_time reste en texte ISO 8601
# puis est converti : un format inattendu donne null au lieu de casser la lecture.
TRIP_SCHEMA = StructType([
    StructField("trip_id", StringType()),
    StructField("user_id", StringType()),
    StructField("vehicle_id", StringType()),
    StructField("vehicle_type", StringType()),
    StructField("start_station", StringType()),
    StructField("end_station", StringType()),
    StructField("city", StringType()),
    StructField("status", StringType()),
    StructField("duration_s", IntegerType()),
    StructField("distance_km", DoubleType()),
    StructField("battery_pct", IntegerType()),
    StructField("price_mad", DoubleType()),
    StructField("event_time", StringType()),
])

STATION_SCHEMA = StructType([
    StructField("station_id", StringType()),
    StructField("name", StringType()),
    StructField("city", StringType()),
    StructField("district", StringType()),
    StructField("zone_type", StringType()),
    StructField("capacity", IntegerType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
])

trips = (spark.read.schema(TRIP_SCHEMA).json(HISTORY)
         .withColumn("event_ts", F.to_timestamp("event_time")))
# multiLine : fonctionne que stations.json soit un tableau JSON ou un objet par ligne
stations = spark.read.schema(STATION_SCHEMA).option("multiLine", True).json(STATIONS)

print("=" * 60)
trips.printSchema()
print(f"Lignes lues : {timed('count(raw)', trips.count)}")
print(f"Stations    : {stations.count()}")

# Trajets terminés, dédoublonnés (même logique qu'en Partie 2)
ended = trips.filter(F.col("status") == "ENDED").dropDuplicates(["trip_id"]).cache()
print(f"Trajets ENDED : {timed('count(ended)', ended.count)}")

# ---------------------------------------------------------------- 2. CA par ville
# API DataFrame
revenue_df = ended.groupBy("city").agg(F.round(F.sum("price_mad"), 2).alias("ca_mad"))
rows = timed("CA par ville (DataFrame)", lambda: sorted(revenue_df.collect()))
print("\nChiffre d'affaires par ville - API DataFrame")
for r in rows:
    print(f"  {r['city']:<12} {r['ca_mad']:>12,.2f}")

# SQL
ended.createOrReplaceTempView("trips_ended")
revenue_sql = spark.sql("""
    SELECT city, ROUND(SUM(price_mad), 2) AS ca_mad
    FROM trips_ended
    GROUP BY city
    ORDER BY city
""")
rows_sql = timed("CA par ville (SQL)", revenue_sql.collect)
print("\nChiffre d'affaires par ville - SQL")
for r in rows_sql:
    print(f"  {r['city']:<12} {r['ca_mad']:>12,.2f}")

# Durée moyenne par ville et type de véhicule, et top 5 (pour comparer avec la Partie 2)
avg_df = (ended.groupBy("city", "vehicle_type")
               .agg(F.round(F.avg("duration_s"), 1).alias("duree_moy_s"))
               .orderBy("city", "vehicle_type"))
avg_rows = timed("Durée moyenne ville x véhicule (DataFrame)", avg_df.collect)
print("\nDurée moyenne d'un trajet (secondes)")
for r in avg_rows:
    print(f"  {r['city']:<12} {r['vehicle_type']:<8} {r['duree_moy_s']:>8.1f}")

top5_df = (ended.groupBy("start_station").count()
                .orderBy(F.desc("count"), "start_station").limit(5))
top5 = timed("Top 5 stations (DataFrame)", top5_df.collect)
print("\nTop 5 des stations de départ")
for r in top5:
    print(f"  {r['start_station']:<12} {r['count']:>6} trajets")

# ---------------------------------------------------------------- 3. Jointure avec les stations
st = stations.select(F.col("station_id"), F.col("district"), F.col("zone_type"),
                     F.col("capacity"))
joined = ended.join(st, ended.start_station == st.station_id, "left")

by_zone = (joined.groupBy("city", "zone_type")
                 .agg(F.count("*").alias("nb_trajets"),
                      F.round(F.sum("price_mad"), 2).alias("ca_mad"))
                 .orderBy("city", F.desc("nb_trajets")))
zone_rows = timed("Jointure + agrégat par zone", by_zone.collect)
print("\nTrajets et CA par ville et type de zone")
for r in zone_rows:
    print(f"  {r['city']:<12} {str(r['zone_type']):<20} {r['nb_trajets']:>6} {r['ca_mad']:>12,.2f}")
orphans = joined.filter(F.col("station_id").isNull()).count()
print(f"Trajets sans station correspondante : {orphans}")

# ---------------------------------------------------------------- 4. UDF vs when/otherwise
def trip_band_py(duration_s):
    """court (< 10 min), moyen (10 à 30 min), long (> 30 min)."""
    if duration_s is None:
        return None
    if duration_s < 600:
        return "court"
    if duration_s <= 1800:
        return "moyen"
    return "long"


trip_band_udf = F.udf(trip_band_py, StringType())
trip_band_col = (F.when(F.col("duration_s").isNull(), F.lit(None))
                  .when(F.col("duration_s") < 600, "court")
                  .when(F.col("duration_s") <= 1800, "moyen")
                  .otherwise("long"))

with_udf = ended.withColumn("trip_band", trip_band_udf("duration_s"))
with_native = ended.withColumn("trip_band", trip_band_col)

band_udf = timed("Répartition trip_band (UDF Python)",
                 lambda: sorted(with_udf.groupBy("trip_band").count().collect()))
band_native = timed("Répartition trip_band (when/otherwise)",
                    lambda: sorted(with_native.groupBy("trip_band").count().collect()))
print("\nRépartition par tranche de durée")
print("  UDF            :", [(r["trip_band"], r["count"]) for r in band_udf])
print("  when/otherwise :", [(r["trip_band"], r["count"]) for r in band_native])
assert [tuple(r) for r in band_udf] == [tuple(r) for r in band_native], "UDF != when/otherwise"

# ---------------------------------------------------------------- 5. Silver : Parquet partitionné par ville
# Tous les statuts (pas seulement ENDED), dédoublonnés sur (trip_id, status) : c'est la
# couche propre que la Partie 6 relira pour construire le graphe.
silver = (trips.dropDuplicates(["trip_id", "status"])
               .withColumn("trip_band", trip_band_col))
timed("Écriture Silver Parquet (partitionBy city)",
      lambda: silver.write.mode("overwrite").partitionBy("city").parquet(SILVER_OUT))
print(f"\nSilver écrit dans {SILVER_OUT} : {spark.read.parquet(SILVER_OUT).count()} lignes")

# ---------------------------------------------------------------- 6. explain(True) sur la jointure
print("\n" + "=" * 60)
print("explain(True) : jointure trajets ENDED x stations")
print("=" * 60)
# Plan volontairement construit sur les données brutes (sans cache ni dédoublonnage) pour
# que la jointure soit lisible : le plan du DataFrame mis en cache serait noyé dans le bruit.
plan_df = (trips.filter(F.col("status") == "ENDED")
                .join(st, F.col("start_station") == F.col("station_id"), "left")
                .groupBy("city", "zone_type").agg(F.count("*").alias("nb_trajets")))
plan_df.explain(True)
try:                                    # copie du plan dans un fichier pour le rapport
    with open(PLAN_FILE, "w", encoding="utf-8") as f:
        f.write(plan_df._jdf.queryExecution().toString())
    print(f"(plan complet enregistré dans {PLAN_FILE})")
except Exception as exc:                # noqa: BLE001 - l'écriture du fichier est facultative
    print(f"(plan non enregistré : {exc})")

# ---------------------------------------------------------------- Récapitulatif des temps
print("\nTemps mesurés (Partie 3) - à comparer avec la Partie 2 (RDD) dans le rapport")
for label, t in timings.items():
    print(f"  {t:6.2f} s  {label}")
print("NB : la 1re action inclut le démarrage des executors et la 1re lecture S3.")

input("\nSpark UI sur http://localhost:4040 - captures DAG / SQL, puis Entrée pour terminer...")
spark.stop()
