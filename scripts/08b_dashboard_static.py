#!/usr/bin/env python3
"""
STEP 8b: static dashboard (matplotlib/seaborn) -> PNG files in outputs/. No browser/port needed,
so it always works inside the lab. Run:  python3 08b_dashboard_static.py
"""
import json
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

from common import OUT_DIR, TX, get_db

sns.set_theme(style="whitegrid")
db = get_db()
agg = lambda p: pd.DataFrame(list(db[TX].aggregate(p, allowDiskUse=True)))

by_type = agg([{"$group": {"_id": "$type", "tx": {"$sum": 1}, "fraud": {"$sum": "$is_fraud"}}}])
by_cat = agg([{"$group": {"_id": "$merchant_category", "tx": {"$sum": 1}, "fraud": {"$sum": "$is_fraud"}}}])
by_cat["rate"] = by_cat.fraud / by_cat.tx * 100
daily = agg([{"$group": {"_id": {"$dateToString": {"format": "%Y-%m-%d", "date": "$ts"}}, "fraud": {"$sum": "$is_fraud"}}},
             {"$sort": {"_id": 1}}])
geo = agg([{"$match": {"is_fraud": 1}}, {"$sample": {"size": 4000}},
           {"$project": {"_id": 0, "lat": {"$arrayElemAt": ["$location.coordinates", 1]},
                         "lon": {"$arrayElemAt": ["$location.coordinates", 0]}}}])
bk = agg([{"$bucket": {"groupBy": "$amount", "boundaries": [0, 1000, 10000, 50000, 100000, 200000, 500000, 1000000, 1000000000],
                       "default": "other", "output": {"tx": {"$sum": 1}, "fraud": {"$sum": "$is_fraud"}}}}])
bk["_id"] = bk["_id"].astype(str)

# ---- Figure 1: overview dashboard (screenshot -> Figure 24)
fig, ax = plt.subplots(2, 2, figsize=(14, 9))
by_type.set_index("_id")[["tx", "fraud"]].plot(kind="bar", ax=ax[0, 0], logy=True, title="Transactions & fraud by type")
sns.barplot(data=by_cat.sort_values("rate"), x="rate", y="_id", ax=ax[0, 1], color="#d62728")
ax[0, 1].set(title="Fraud rate (%) by merchant category", xlabel="fraud rate %", ylabel="")
bk.set_index("_id")[["tx", "fraud"]].plot(kind="bar", ax=ax[1, 0], logy=True, title="Amount bands ($bucket)")
ax[1, 1].axis("off")
tot, fr = int(by_type.tx.sum()), int(by_type.fraud.sum())
ax[1, 1].text(0.05, 0.6, f"Transactions : {tot:,}\nFraud         : {fr:,}\nFraud rate    : {100 * fr / tot:.2f}%",
              fontsize=20, family="monospace")
ax[1, 1].set_title("KPIs")
fig.suptitle("FraudLens - Overview", fontsize=16); fig.tight_layout()
fig.savefig(os.path.join(OUT_DIR, "dashboard_1_overview.png"), dpi=130)

# ---- Figure 2: geo & time (-> Figure 25)
fig, ax = plt.subplots(1, 2, figsize=(14, 5))
ax[0].plot(daily["_id"], daily["fraud"], marker="o"); ax[0].set_title("Daily fraud count")
ax[0].tick_params(axis="x", rotation=90); ax[0].xaxis.set_major_locator(plt.MaxNLocator(10))
ax[1].scatter(geo.lon, geo.lat, s=6, alpha=.4, c="#d62728"); ax[1].set(title="Fraud locations (sample)", xlabel="lon", ylabel="lat")
fig.tight_layout(); fig.savefig(os.path.join(OUT_DIR, "dashboard_2_geo_time.png"), dpi=130)

# ---- Figure 3: model results (-> Figure 26)
mp, fp, cp = [os.path.join(OUT_DIR, f) for f in ("metrics.json", "feature_importance.csv", "compare_results.csv")]
if os.path.exists(mp) and os.path.exists(fp):
    mt = json.load(open(mp)); fi = pd.read_csv(fp).head(12).sort_values("importance")
    fig, ax = plt.subplots(1, 2, figsize=(14, 5))
    pd.DataFrame(mt["models"]).T[["auc_roc", "auc_pr", "precision", "recall", "f1"]].plot(kind="bar", ax=ax[0], title="Model metrics")
    ax[0].set_ylim(0, 1.05); ax[0].tick_params(axis="x", rotation=0)
    ax[1].barh(fi.feature, fi.importance, color="#1f77b4"); ax[1].set_title(f"Feature importance ({mt['best_model']})")
    fig.tight_layout(); fig.savefig(os.path.join(OUT_DIR, "dashboard_3_model.png"), dpi=130)
else:
    print("[WARN] run step 6 first for the model dashboard")
print("[DONE] PNGs saved in outputs/ : dashboard_1_overview.png, dashboard_2_geo_time.png, dashboard_3_model.png")
