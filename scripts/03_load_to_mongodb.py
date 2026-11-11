#!/usr/bin/env python3
"""
STEP 3 (run in the lab): load data/fraud_tx.jsonl.gz into MongoDB, create indexes,
and build the 'devices' collection with an in-database aggregation ($group + $merge).

Usage:  python3 03_load_to_mongodb.py [--input ../data/fraud_tx.jsonl.gz] [--keep]
"""
import argparse
import gzip
import json
import os
import time
from datetime import datetime

from pymongo import ASCENDING, GEOSPHERE

from common import DB_NAME, DEVICES, MONGO_URI, ROOT, TX, get_db

ap = argparse.ArgumentParser()
ap.add_argument("--input", default=os.path.join(ROOT, "data", "fraud_tx.jsonl.gz"))
ap.add_argument("--keep", action="store_true", help="do not drop existing collection first")
a = ap.parse_args()

db = get_db()
print(f"[INFO] MongoDB {db.client.server_info()['version']} | uri={MONGO_URI} | db={DB_NAME}")
if not a.keep:
    db[TX].drop()
    db[DEVICES].drop()

t0 = time.time()
batch, total = [], 0
with gzip.open(a.input, "rt", encoding="utf-8") as f:
    for line in f:
        d = json.loads(line)
        d["ts"] = datetime.strptime(d["ts"], "%Y-%m-%dT%H:%M:%S")   # real BSON Date
        batch.append(d)
        if len(batch) == 10000:
            db[TX].insert_many(batch, ordered=False)
            total += len(batch)
            batch = []
            print(f"  loaded {total:,}", end="\r")
if batch:
    db[TX].insert_many(batch, ordered=False)
    total += len(batch)
print(f"\n[DONE] Inserted {total:,} documents in {time.time() - t0:.1f}s")

print("[INFO] Creating indexes ...")
t1 = time.time()
db[TX].create_index([("origin.account", ASCENDING)])
db[TX].create_index([("dest.account", ASCENDING)])
db[TX].create_index([("device.id", ASCENDING), ("step", ASCENDING)])
db[TX].create_index([("ts", ASCENDING)])
db[TX].create_index([("is_fraud", ASCENDING), ("type", ASCENDING)])
db[TX].create_index([("location", GEOSPHERE)])
print(f"[DONE] Indexes created in {time.time() - t1:.1f}s")
for ix in db[TX].list_indexes():
    print("   ", ix["name"], dict(ix["key"]))

print("[INFO] Building 'devices' collection with $group + $merge (compute inside MongoDB) ...")
db[TX].aggregate([
    {"$group": {"_id": "$device.id", "tx_count": {"$sum": 1}, "fraud_count": {"$sum": "$is_fraud"},
                "total_amount": {"$sum": "$amount"}, "countries": {"$addToSet": "$country"}}},
    {"$addFields": {"fraud_rate": {"$divide": ["$fraud_count", "$tx_count"]}}},
    {"$merge": {"into": DEVICES, "whenMatched": "replace", "whenNotMatched": "insert"}},
], allowDiskUse=True)

print("\n=========== LOAD SUMMARY (screenshot this) ===========")
print("transactions :", db[TX].count_documents({}))
print("fraud txns   :", db[TX].count_documents({"is_fraud": 1}))
print("devices      :", db[DEVICES].count_documents({}))
st = db.command("collStats", TX)
print(f"data size    : {st['size'] / 1e6:.1f} MB | storage: {st['storageSize'] / 1e6:.1f} MB | "
      f"index size: {st['totalIndexSize'] / 1e6:.1f} MB")
