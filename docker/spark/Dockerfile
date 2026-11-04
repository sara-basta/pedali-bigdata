# Image unique pour Master, Workers et Jupyter : Python 3.11 + Java 17 + Spark 3.5.3
# (même Python sur le driver et les executors, sinon PySpark refuse de démarrer)
FROM python:3.11-slim-bookworm

RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps curl \
 && rm -rf /var/lib/apt/lists/* \
 && ln -s "$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" /opt/java

# pyspark (pip) contient Spark complet : jars + spark-class, spark-submit, spark-shell
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

ENV JAVA_HOME=/opt/java \
    SPARK_HOME=/usr/local/lib/python3.11/site-packages/pyspark \
    PATH="/usr/local/lib/python3.11/site-packages/pyspark/bin:${PATH}" \
    PYSPARK_PYTHON=python3.11 \
    PYTHONPATH=/opt/pedali/notebooks

WORKDIR /opt/pedali
