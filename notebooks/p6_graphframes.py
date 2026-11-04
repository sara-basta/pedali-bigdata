"""Partie 6 - Analyse de graphes avec GraphFrames (PySpark)."""
from pedali_common import GOLD, PKG_GRAPHFRAMES, SILVER, get_spark

spark = get_spark("pedali-p6", [PKG_GRAPHFRAMES])

# TODO 1. Sommets (stations) et arêtes TRIP agrégées (nb trajets, durée, distance, period)
#         - exclure les arêtes orphelines, traiter à part les boucles
# TODO 2. GraphFrame(vertices, edges) + contrôles par ville
# TODO 3. Motifs find() : allers-retours, boucles de 3 stations, corridors
#         - degrés pondérés et solde net (arrivées - départs) par station et période
# TODO 5. Comparaison avec GraphX (graphx/GraphAnalysis.scala) : degrés, PageRank, temps
# TODO 6. Recommandations de rééquilibrage (seuils explicites, couples source -> cible)
# TODO 7. Export Parquet lake/gold/graph_analysis/ (par ville et type de résultat) + visualisation

spark.stop()
