#!/usr/bin/env python3
"""
STEP 6 (PySpark MLlib): Random Forest + Gradient-Boosted Trees fraud models, class-weighted for
imbalance. Saves metrics + feature importance and writes the best model's fraud scores BACK to MongoDB
(collection 'fraud_predictions')  -> completes the Track-C hybrid loop.
Run with:  bash run_spark.sh 06_pyspark_ml_pipeline.py
"""
import json
import os
import time

from pyspark.ml import Pipeline
from pyspark.ml.classification import GBTClassifier, RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import functions as F

from common import DB_NAME, OUT_DIR, PRED, get_db, get_spark, read_transactions, write_collection
from features import CAT_COLS, NUM_COLS, add_features, feature_names, flatten

spark = get_spark("FraudLens-06-MLlib")
df, mode = read_transactions(spark)
data = add_features(flatten(df)).cache()

train, test = data.randomSplit([0.8, 0.2], seed=42)
n_pos = train.filter("label = 1").count()
n_neg = train.filter("label = 0").count()
ratio = n_neg / max(n_pos, 1)
print(f"\nTrain rows: {train.count():,} | test rows: {test.count():,} | fraud in train: {n_pos:,} | class weight: {ratio:.1f}")
train = train.withColumn("w", F.when(F.col("label") == 1, F.lit(ratio)).otherwise(F.lit(1.0)))

prep = [StringIndexer(inputCol=c, outputCol=c + "_idx", handleInvalid="keep") for c in CAT_COLS]
prep.append(OneHotEncoder(inputCols=[c + "_idx" for c in CAT_COLS], outputCols=[c + "_vec" for c in CAT_COLS]))
prep.append(VectorAssembler(inputCols=NUM_COLS + [c + "_vec" for c in CAT_COLS],
                            outputCol="features", handleInvalid="keep"))

models = {
    "RandomForest": RandomForestClassifier(labelCol="label", featuresCol="features", weightCol="w",
                                           numTrees=80, maxDepth=8, seed=42),
    "GBT": GBTClassifier(labelCol="label", featuresCol="features", weightCol="w", maxIter=40, maxDepth=5, seed=42),
}
ev_roc = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC")
ev_pr = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderPR")

results, fitted = {}, {}
for name, clf in models.items():
    t0 = time.time()
    model = Pipeline(stages=prep + [clf]).fit(train)
    secs = time.time() - t0
    pred = model.transform(test).cache()
    cm = {(r["label"], int(r["prediction"])): r["count"] for r in pred.groupBy("label", "prediction").count().collect()}
    tp, fp, fn, tn = cm.get((1, 1), 0), cm.get((0, 1), 0), cm.get((1, 0), 0), cm.get((0, 0), 0)
    prec = tp / (tp + fp) if tp + fp else 0.0
    rec = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * prec * rec / (prec + rec) if prec + rec else 0.0
    results[name] = {"auc_roc": round(ev_roc.evaluate(pred), 4), "auc_pr": round(ev_pr.evaluate(pred), 4),
                     "precision": round(prec, 4), "recall": round(rec, 4), "f1": round(f1, 4),
                     "tp": tp, "fp": fp, "fn": fn, "tn": tn, "train_seconds": round(secs, 1)}
    fitted[name] = (model, pred)
    print(f"\n===== {name} =====")
    print(json.dumps(results[name], indent=2))
    print(f"Confusion matrix (rows=actual, cols=predicted):\n   TN={tn:<8} FP={fp}\n   FN={fn:<8} TP={tp}")

best = max(results, key=lambda k: results[k]["auc_pr"])
print(f"\n>>> Best model by AUC-PR: {best}")
best_model, best_pred = fitted[best]

# feature importance
imp = best_model.stages[-1].featureImportances.toArray()
names = feature_names(best_pred)
fi = sorted(zip(names, imp), key=lambda x: -x[1])
print("\n===== TOP 10 FEATURE IMPORTANCES =====")
for n_, v in fi[:10]:
    print(f"  {n_:<35s} {v:.4f}")
with open(os.path.join(OUT_DIR, "feature_importance.csv"), "w") as f:
    f.write("feature,importance\n")
    for n_, v in fi:
        f.write(f"{n_},{v:.6f}\n")

with open(os.path.join(OUT_DIR, "metrics.json"), "w") as f:
    json.dump({"read_mode": mode, "best_model": best, "train_fraud": n_pos, "class_weight": round(ratio, 2),
               "models": results}, f, indent=2)

# ---- write predictions back to MongoDB (hybrid loop) ----
out = best_pred.select("tx_id", "label", F.col("prediction").cast("int").alias("prediction"),
                       vector_to_array("probability")[1].alias("fraud_score"), F.lit(best).alias("model"))
write_collection(out, PRED)
db = get_db()
db[PRED].create_index("tx_id")
db[PRED].create_index([("fraud_score", -1)])
print(f"\n[DONE] '{PRED}' now has {db[PRED].count_documents({}):,} documents in MongoDB db '{DB_NAME}'")
print("Check in mongosh:  db.fraud_predictions.find().sort({fraud_score:-1}).limit(5)")
spark.stop()
