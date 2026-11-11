"""
common.py - shared configuration + helpers for every script.

Change MONGO_URI if MongoDB is not on localhost of the node you run from, e.g.
    export MONGO_URI="mongodb://master:27017"
"""
import json
import os

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = os.environ.get("MONGO_DB", "fraudlens")
TX = "transactions"
DEVICES = "devices"
PRED = "fraud_predictions"

ROOT = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
OUT_DIR = os.path.join(ROOT, "outputs")
os.makedirs(OUT_DIR, exist_ok=True)


def get_db():
    from pymongo import MongoClient
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    client.admin.command("ping")          # fail fast with a clear error if Mongo is down
    return client[DB_NAME]


def get_spark(app="FraudLens"):
    from pyspark.sql import SparkSession
    spark = (SparkSession.builder.appName(app)
             .config("spark.mongodb.read.connection.uri", MONGO_URI)
             .config("spark.mongodb.write.connection.uri", MONGO_URI)
             .config("spark.sql.shuffle.partitions", "16")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_transactions(spark):
    """Read the transactions collection into a Spark DataFrame (nested schema).
    1st choice : Mongo-Spark Connector 10.x  (format 'mongodb')  -> TRUE Track-C hybrid
    fallback   : pymongo -> JSON strings -> spark.read.json      (used if the connector jar is missing)
    Returns (df, mode_string)
    """
    if os.environ.get("USE_CONNECTOR", "1") == "1":
        try:
            df = (spark.read.format("mongodb")
                  .option("database", DB_NAME).option("collection", TX).load())
            df.limit(1).count()                       # force connection now
            print("[INFO] >>> Read via Mongo-Spark Connector <<<")
            return df.drop("_id"), "mongo-spark-connector"
        except Exception as e:                        # noqa
            print("[WARN] Mongo-Spark connector not usable -> falling back to pymongo.\n       reason:",
                  str(e).splitlines()[0][:200])
    db = get_db()
    rows = []
    for d in db[TX].find({}, {"_id": 0}):
        d["ts"] = d["ts"].isoformat()
        rows.append(json.dumps(d))
    df = spark.read.json(spark.sparkContext.parallelize(rows, 16))
    print("[INFO] >>> Read via pymongo fallback <<<")
    return df, "pymongo-fallback"


def write_collection(df, name, mode_hint="mongo-spark-connector"):
    """Write a Spark DataFrame to a MongoDB collection (replaces the collection)."""
    db = get_db()
    db[name].drop()
    if os.environ.get("USE_CONNECTOR", "1") == "1":
        try:
            (df.write.format("mongodb").mode("append")
               .option("database", DB_NAME).option("collection", name).save())
            print(f"[INFO] Wrote '{name}' via Mongo-Spark Connector")
            return "mongo-spark-connector"
        except Exception as e:                        # noqa
            print("[WARN] connector write failed -> pymongo fallback.\n       reason:",
                  str(e).splitlines()[0][:200])
            db[name].drop()
    recs = df.toPandas().to_dict("records")
    for i in range(0, len(recs), 10000):
        db[name].insert_many(recs[i:i + 10000])
    print(f"[INFO] Wrote '{name}' via pymongo fallback ({len(recs)} docs)")
    return "pymongo-fallback"
