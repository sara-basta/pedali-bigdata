# Pedali.ma – Plateforme Big Data

Projet *Écosystème Big Data 2* (EMSI, 5ème année), réalisé en binôme.

```
producer.py → Kafka (topic trips) → Spark Structured Streaming → MinIO (Bronze → Silver → Gold)
                                                               → GraphX / GraphFrames
```

## Versions

| Composant | Version |
|---|---|
| Python | 3.11 |
| Spark | 3.5.3 (Scala 2.12, Java 17) |
| Kafka | 3.8.0 (KRaft) |
| MinIO | `bitnamilegacy/minio` 2025.7.23 (plus d'image officielle MinIO) |
| Kafka connector | `spark-sql-kafka-0-10_2.12:3.5.3` |
| S3A | `hadoop-aws:3.3.4` |
| Delta Lake | `delta-spark_2.12:3.2.1` |
| GraphFrames | `0.8.4-spark3.5-s_2.12` |

## Arborescence

```
docker-compose.yml          Kafka, Spark Master + 2 Workers, MinIO, Jupyter
docker/spark/               image Spark/Jupyter (Dockerfile + requirements.txt)
data/                       stations.json (fourni)
producer/                   producer.py (fourni)
notebooks/
  pedali_common.py          SparkSession + chemins du lake
  p1_cluster.py … p6_graphframes.py
graphx/GraphAnalysis.scala  Partie 6 – GraphX
report/captures/            captures pour le rapport
```

## Démarrage

```bash
docker compose up -d --build
```

| Interface | URL |
|---|---|
| Spark Master | http://localhost:8080 |
| Jupyter Lab | http://localhost:8888 |
| Spark UI (application) | http://localhost:4040 |
| Console MinIO (minioadmin / minioadmin) | http://localhost:9001 |

Le bucket `lake` et le topic `trips` (3 partitions) sont créés au démarrage.

Lancer une partie :

```bash
docker exec -it jupyter spark-submit notebooks/p1_cluster.py
```

## Échéances

- Jalon 1 (Parties 1–3) : 22/11/2026
- Jalon 2 (Parties 4–6 + soutenance) : 20/12/2026
