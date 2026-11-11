#!/bin/bash
# Wrapper to submit a PySpark script with the Mongo-Spark connector.
#   bash run_spark.sh 05_pyspark_eda_features.py
#
# Environment variables you can set before running:
#   SPARK_MASTER   default local[*]      (use "yarn" to show the cluster in your report)
#   CONNECTOR_JAR  path to a local mongo-spark-connector jar (use if the lab has no internet)
#   CONNECTOR_PKG  default org.mongodb.spark:mongo-spark-connector_2.12:10.4.0
#                  (use _2.13 build only if 'spark-submit --version' shows Scala 2.13)
#   MONGO_URI      default mongodb://localhost:27017
#   USE_CONNECTOR  set to 0 to skip the connector and use the pymongo fallback directly
cd "$(dirname "$0")"
SCRIPT="$1"; [ -z "$SCRIPT" ] && { echo "usage: bash run_spark.sh <script.py>"; exit 1; }
MASTER="${SPARK_MASTER:-local[*]}"
PKG="${CONNECTOR_PKG:-org.mongodb.spark:mongo-spark-connector_2.12:10.4.0}"

if [ -n "$CONNECTOR_JAR" ]; then DEPS="--jars $CONNECTOR_JAR"; else DEPS="--packages $PKG"; fi
[ "$USE_CONNECTOR" = "0" ] && DEPS=""

echo ">>> spark-submit --master $MASTER $DEPS $SCRIPT"
spark-submit --master "$MASTER" --driver-memory "${DRIVER_MEM:-2g}" $DEPS "$SCRIPT" 2>&1 | grep -v -E "^(WARNING|SLF4J)" | tee "../outputs/log_${SCRIPT%.py}.txt"
