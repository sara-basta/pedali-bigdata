// Partie 6 - GraphX (Scala), à lancer dans spark-shell :
//   docker exec -it jupyter spark-shell --master spark://spark-master:7077 -i /opt/pedali/graphx/GraphAnalysis.scala

import org.apache.spark.graphx._

// TODO 4. Lire les sommets / arêtes exportés en Parquet
// TODO    Associer un VertexId (Long) unique à chaque station
// TODO    Construire Graph(verticesRDD, edgesRDD)
// TODO    degrees, pageRank, connectedComponents
// TODO    ShortestPaths vers la gare principale de chaque ville
