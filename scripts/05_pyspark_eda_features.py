#!/usr/bin/env python3
"""
STEP 5 (PySpark): read nested documents from MongoDB, flatten the schema, run windowed
aggregations and basic EDA.   Run with:  bash run_spark.sh 05_pyspark_eda_features.py
"""
import os
import time

from pyspark.sql import functions as F

from common import OUT_DIR, get_spark, read_transactions
from features import add_features, flatten

spark = get_spark("FraudLens-05-EDA")
t0 = time.time()
df, mode = read_transactions(spark)
print(f"\n===== NESTED SCHEMA read from MongoDB (mode: {mode}) =====")
df.printSchema()
print("Total rows:", df.count())

flat = flatten(df)
print("\n===== FLATTENED SCHEMA =====")
flat.printSchema()

feat = add_features(flat).cache()
print("\n===== SAMPLE WITH WINDOW FEATURES (device 24h rolling window) =====")
feat.select("tx_id", "type", "amount", "device_id", "device_tx_24h", "device_avg_amt_24h",
            "geo_distance_km", "country_mismatch", "label").orderBy(F.desc("device_tx_24h")).show(10, truncate=False)

print("\n===== EDA 1: counts & fraud rate by type =====")
t = (feat.groupBy("type").agg(F.count("*").alias("tx"), F.sum("label").alias("fraud"),
                              F.round(F.avg("amount"), 2).alias("avg_amount"))
     .withColumn("fraud_rate_pct", F.round(F.col("fraud") / F.col("tx") * 100, 3)).orderBy(F.desc("fraud_rate_pct")))
t.show()
t.toPandas().to_csv(os.path.join(OUT_DIR, "eda_by_type.csv"), index=False)

print("\n===== EDA 2: feature averages - fraud vs normal =====")
g = feat.groupBy("label").agg(*[F.round(F.avg(c), 3).alias(c) for c in
                                ["amount", "geo_distance_km", "country_mismatch", "device_tx_24h", "dest_tx_24h"]])
g.show()
g.toPandas().to_csv(os.path.join(OUT_DIR, "eda_fraud_vs_normal.csv"), index=False)

print("\n===== EDA 3: fraud by hour of day =====")
h = (feat.groupBy("hour_of_day").agg(F.count("*").alias("tx"), F.sum("label").alias("fraud"))
     .orderBy("hour_of_day"))
h.show(24)
h.toPandas().to_csv(os.path.join(OUT_DIR, "eda_by_hour.csv"), index=False)
print(f"\n[DONE] step 5 finished in {time.time() - t0:.1f}s | Spark UI: http://<node>:4040 while running")
spark.stop()
