# Pedali.ma – Plateforme Big Data

Projet *Écosystème Big Data 2* (EMSI, 5ème année), réalisé en binôme par **Sara Basta** et
**Siham Chaouy**.

Pedali.ma est un service fictif de vélos et trottinettes en libre-service (Casablanca, Rabat,
Marrakech, Tanger). La plateforme analyse les trajets entre stations avec Spark, Kafka,
MinIO, GraphX et GraphFrames.

```
producer.py → Kafka (topic trips) → Spark Structured Streaming → MinIO (Bronze → Silver → Gold)
                                                               → GraphX / GraphFrames
```

## Avancement

| Partie | Contenu | Script | État |
|---|---|---|---|
| 1 | Cluster Docker, SparkSession, positionnement | `notebooks/p1_cluster.py` | ✅ Jalon 1 |
| 2 | Batch SparkCore (RDD) | `notebooks/p2_rdd.py` | ✅ Jalon 1 |
| 3 | Spark SQL et DataFrames, Silver Parquet | `notebooks/p3_dataframes.py` | ✅ Jalon 1 |
| 4 | Structured Streaming depuis Kafka | `notebooks/p4_streaming.py` | ⏳ Jalon 2 |
| 5 | Pipeline de production (Silver / Gold Delta) | `notebooks/p5_pipeline.py` | ⏳ Jalon 2 |
| 6 | GraphFrames (PySpark) et GraphX (Scala) | `notebooks/p6_graphframes.py`, `graphx/GraphAnalysis.scala` | ⏳ Jalon 2 |

Rapport du jalon 1 : [`report/rapport.pdf`](report/rapport.pdf) (source LaTeX : `report/rapport.tex`).

## Versions

| Composant | Version |
|---|---|
| Python (driver et executors) | 3.11 |
| Spark / PySpark | 3.5.3 (Scala 2.12, Java 17) |
| Kafka | 3.8.0 (mode KRaft, sans ZooKeeper) |
| MinIO | `bitnamilegacy/minio:2025.7.23` (il n'y a plus d'image officielle MinIO) |
| Console MinIO | `bitnamilegacy/minio-object-browser:2.0.2` |
| Connecteur S3A | `hadoop-aws:3.3.4` + `aws-java-sdk-bundle:1.12.262` |
| Connecteur Kafka | `spark-sql-kafka-0-10_2.12:3.5.3` |
| Delta Lake | `delta-spark_2.12:3.2.1` |
| GraphFrames | `0.8.4-spark3.5-s_2.12` |
| Dépendances Python | `docker/spark/requirements.txt` (pyspark, jupyterlab, pandas, pyarrow, kafka-python) |

Les jars Java (S3A, Kafka, Delta, GraphFrames) ne sont pas dans l'image. Spark les télécharge
au premier lancement via `spark.jars.packages`, défini dans `notebooks/pedali_common.py`.
Ce premier lancement demande donc un accès Internet.

## Prérequis

- Docker Desktop (ou Docker Engine + Compose v2), avec environ 6 Go de RAM disponibles pour
  les conteneurs.
- Ports libres : 8080–8082, 8888, 4040, 9000, 9001, 9094.
- Rien à installer sur la machine hôte : tous les scripts tournent dans le conteneur `jupyter`.

## Arborescence

```
docker-compose.yml          Kafka, Spark Master + 2 Workers, MinIO (+ console), Jupyter
docker/spark/               image Spark/Jupyter (Dockerfile + requirements.txt)
data/                       stations.json et trips_history.jsonl, produits par producer/ (le .jsonl n'est pas versionné)
producer/
  gen_fake_history.py       génère trips_history.jsonl (schéma de l'énoncé, 5 % retards, 2 % doublons)
  gen_fake_stations.py      génère stations.json (400 stations, 4 villes)
  upload_to_minio.py        copie un fichier local dans le bucket lake
notebooks/
  pedali_common.py          SparkSession (Master + S3A/MinIO) et chemins du lake
  p1_cluster.py … p6_graphframes.py
graphx/GraphAnalysis.scala  Partie 6 – GraphX
report/
  rapport.tex / rapport.pdf rapport du jalon 1
  captures/                 captures Spark UI, Spark Master, MinIO
  sorties/                  sorties console brutes des parties 2 et 3, explain(True), mc ls
```

## Démarrage

```bash
docker compose up -d --build
```

Le service `kafka-init` crée le topic `trips` (3 partitions). MinIO crée le bucket `lake`
au démarrage.

| Interface | URL |
|---|---|
| Spark Master | http://localhost:8080 |
| Spark UI de l'application en cours | http://localhost:4040 |
| Jupyter Lab | http://localhost:8888 |
| Console MinIO (`minioadmin` / `minioadmin`) | http://localhost:9001 |

Pour changer les identifiants MinIO, copier `.env.example` en `.env`.

> **Important :** lancer les scripts avec `python`, **pas avec `spark-submit`**. Les jars sont
> déclarés dans `SparkSession.builder` (`spark.jars.packages`). Ce réglage n'est pris en
> compte que si c'est PySpark qui démarre la JVM. Avec `spark-submit`, la JVM existe déjà :
> S3A est alors introuvable (`ClassNotFoundException: S3AFileSystem`).

## Jalon 1 – Parties 1 à 3

### 1. Préparer les données

Les générateurs sont déterministes (graines fixes) : ils redonnent toujours le même jeu de
51 000 lignes et 400 stations.

```bash
docker exec -it jupyter python producer/gen_fake_history.py --n 50000
```

```bash
docker exec -it jupyter python producer/gen_fake_stations.py
```

```bash
docker exec -it jupyter python producer/upload_to_minio.py
```

```bash
docker exec -it jupyter python producer/upload_to_minio.py data/stations.json lake/bronze/ref/stations.json
```

Si vous utilisez le `producer.py` de l'énoncé, remplacez la première commande par
`producer.py --mode file --n 50000`, puis placez `stations.json` dans `data/`.

### 2. Partie 1 – Cluster

```bash
docker exec -it jupyter python notebooks/p1_cluster.py
```

Affiche la version de Spark, le master, l'appName, `defaultParallelism` (= 4, soit 2 Workers
× 2 cœurs) et les deux executors.

### 3. Partie 2 – RDD

```bash
docker exec -it jupyter python notebooks/p2_rdd.py
```

Affiche le CA par ville, la durée moyenne par ville et type de véhicule, le top 5 des
stations de départ et le lineage (`toDebugString`). Le script reste ouvert à la fin pour
laisser le temps de capturer le DAG sur http://localhost:4040 ; appuyer sur Entrée pour le
terminer.

### 4. Partie 3 – DataFrames et SQL

```bash
docker exec -it jupyter python notebooks/p3_dataframes.py
```

Le script lit les données avec un schéma explicite et refait le calcul du CA (API DataFrame
puis SQL). Il fait ensuite la jointure avec les stations (par ville et type de zone) et
compare l'UDF `trip_band` à `when/otherwise`. Il écrit la couche Silver dans
`lake/silver/trips_history/` (Parquet partitionné par `city`). Enfin, il affiche
`explain(True)`, copié dans `data/p3_explain.txt`, et le récapitulatif des temps.

Vérifier le contenu du lake :

```bash
docker exec minio sh -c 'mc alias set local http://localhost:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" && mc ls --recursive local/lake/'
```

### Résultats de référence (jeu généré par défaut)

| Ville | CA (MAD) | Trajets terminés |
|---|---:|---:|
| Casablanca | 192 559,97 | 6 447 |
| Rabat | 86 641,49 | 2 918 |
| Marrakech | 83 975,32 | 2 813 |
| Tanger | 66 386,47 | 2 192 |

51 000 lignes lues, 14 370 trajets terminés distincts, 50 000 lignes dans la couche Silver.
Les sorties complètes sont dans `report/sorties/`.

## Jalon 2 – Parties 4 à 6 (à venir)

```bash
docker exec -it jupyter python notebooks/p4_streaming.py
```

```bash
docker exec -it jupyter python notebooks/p5_pipeline.py
```

```bash
docker exec -it jupyter python notebooks/p6_graphframes.py
```

```bash
docker exec -it jupyter spark-shell --master spark://spark-master:7077 -i /opt/pedali/graphx/GraphAnalysis.scala
```

Ces scripts ne sont encore que des squelettes : les étapes à réaliser y sont listées en `TODO`.

## Compiler le rapport

Depuis `report/`, avec une distribution LaTeX (MiKTeX, TeX Live) ou sur Overleaf en
important tout le dossier `report/` :

```bash
pdflatex rapport.tex && pdflatex rapport.tex
```

## Dépannage

- **`ClassNotFoundException: S3AFileSystem`** : le script a été lancé avec `spark-submit`.
  Relancer avec `python` (voir plus haut).
- **Git Bash sous Windows** : il réécrit les chemins `/opt/...` passés à `docker exec`.
  Préfixer la commande par `MSYS_NO_PATHCONV=1`, ou utiliser PowerShell.
- **Scripts `.py` en CRLF** : `.gitattributes` force les fins de ligne LF, même depuis
  Windows.