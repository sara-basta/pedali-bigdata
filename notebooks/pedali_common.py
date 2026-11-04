"""Configuration commune : chemins du lake et création de la SparkSession."""
import os

from pyspark.sql import SparkSession

# Jars compatibles Spark 3.5.3 / Scala 2.12
PKG_S3A = "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
PKG_KAFKA = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"
PKG_DELTA = "io.delta:delta-spark_2.12:3.2.1"
PKG_GRAPHFRAMES = "graphframes:graphframes:0.8.4-spark3.5-s_2.12"

LAKE = "s3a://lake"
BRONZE = f"{LAKE}/bronze"
SILVER = f"{LAKE}/silver"
GOLD = f"{LAKE}/gold"
CHECKPOINTS = f"{LAKE}/checkpoints"

KAFKA_BOOTSTRAP = "kafka:9092"
TOPIC = "trips"


def get_spark(app_name, packages=()):
    """SparkSession connectée au Master, avec accès à MinIO (S3A).

    packages : jars supplémentaires, ex. get_spark("p4", [PKG_KAFKA])
    """
    return (SparkSession.builder
            .appName(app_name)
            .master("spark://spark-master:7077")
            .config("spark.driver.host", "jupyter")
            .config("spark.jars.packages", ",".join([PKG_S3A, *packages]))
            .config("spark.jars.repositories", "https://repos.spark-packages.org/")
            .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
            .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER", "minioadmin"))
            .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"))
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .getOrCreate())
