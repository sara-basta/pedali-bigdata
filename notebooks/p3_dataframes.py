"""Partie 3 - Spark SQL et DataFrames."""
from pedali_common import BRONZE, SILVER, get_spark

spark = get_spark("pedali-p3")

# TODO 1. Lire trips_history.jsonl et stations.json avec un schéma explicite (StructType)
# TODO 2. CA par ville en API DataFrame, puis en SQL (createOrReplaceTempView + spark.sql)
# TODO 3. Jointure trajets ENDED x stations : nb trajets et CA par type de zone et ville
# TODO 4. UDF trip_band(duration_s) vs when/otherwise
# TODO 5. Écrire en Parquet dans lake/silver/trips_history/, partitionné par city
# TODO 6. explain(True) sur la jointure

# Rapport : plans logique/optimisé/physique, RDD vs DataFrame vs Dataset (temps mesurés)

spark.stop()
