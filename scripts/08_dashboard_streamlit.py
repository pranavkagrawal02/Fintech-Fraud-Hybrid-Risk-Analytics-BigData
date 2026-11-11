#!/usr/bin/env python3
"""
STEP 8a: interactive dashboard (Streamlit + Plotly) reading live from MongoDB.
Run:  streamlit run 08_dashboard_streamlit.py --server.port 8501 --server.address 0.0.0.0
(If you cannot open the port from your browser in the lab, use 08b_dashboard_static.py instead.)
"""
import json
import os

import pandas as pd
import plotly.express as px
import streamlit as st

from common import OUT_DIR, PRED, TX, get_db

st.set_page_config(page_title="FraudLens Dashboard", layout="wide")
db = get_db()


@st.cache_data(ttl=300)
def agg(coll, pipeline):
    return pd.DataFrame(list(db[coll].aggregate(pipeline, allowDiskUse=True)))


st.title("FraudLens - Real-time Fraud & Geospatial Risk Dashboard")
st.caption("MongoDB (operational store) + PySpark MLlib (model training) - hybrid architecture")

tot = agg(TX, [{"$group": {"_id": None, "tx": {"$sum": 1}, "fraud": {"$sum": "$is_fraud"},
                           "amt": {"$sum": "$amount"},
                           "fraud_amt": {"$sum": {"$cond": [{"$eq": ["$is_fraud", 1]}, "$amount", 0]}}}}]).iloc[0]
c1, c2, c3, c4 = st.columns(4)
c1.metric("Transactions", f"{int(tot.tx):,}")
c2.metric("Fraud transactions", f"{int(tot.fraud):,}")
c3.metric("Fraud rate", f"{100 * tot.fraud / tot.tx:.2f}%")
c4.metric("Fraud amount", f"{tot.fraud_amt:,.0f}")

tab1, tab2, tab3, tab4 = st.tabs(["Overview", "Geo & Time", "ML Model", "Mongo vs Spark"])

with tab1:
    a, b = st.columns(2)
    t = agg(TX, [{"$group": {"_id": "$type", "tx": {"$sum": 1}, "fraud": {"$sum": "$is_fraud"}}}])
    a.plotly_chart(px.bar(t, x="_id", y=["tx", "fraud"], barmode="group", title="Transactions & fraud by type",
                          labels={"_id": "type", "value": "count"}), use_container_width=True)
    m = agg(TX, [{"$group": {"_id": "$merchant_category", "tx": {"$sum": 1}, "fraud": {"$sum": "$is_fraud"}}},
                 {"$addFields": {"fraud_rate_pct": {"$multiply": [{"$divide": ["$fraud", "$tx"]}, 100]}}}])
    b.plotly_chart(px.bar(m.sort_values("fraud_rate_pct"), x="fraud_rate_pct", y="_id", orientation="h",
                          title="Fraud rate (%) by merchant category", labels={"_id": "category"}),
                   use_container_width=True)
    bk = agg(TX, [{"$bucket": {"groupBy": "$amount",
                               "boundaries": [0, 1000, 10000, 50000, 100000, 200000, 500000, 1000000, 1000000000],
                               "default": "other", "output": {"tx": {"$sum": 1}, "fraud": {"$sum": "$is_fraud"}}}}])
    bk["_id"] = bk["_id"].astype(str)
    st.plotly_chart(px.bar(bk, x="_id", y=["tx", "fraud"], barmode="group", log_y=True,
                           title="Amount bands (lower bound) - $bucket", labels={"_id": "amount >="}),
                    use_container_width=True)
    dv = agg("devices", [{"$match": {"tx_count": {"$gte": 5}}}, {"$sort": {"fraud_rate": -1, "tx_count": -1}},
                         {"$limit": 10}, {"$project": {"countries": 0}}])
    st.subheader("Top-10 riskiest devices")
    st.dataframe(dv, use_container_width=True)

with tab2:
    d = agg(TX, [{"$group": {"_id": {"$dateToString": {"format": "%Y-%m-%d", "date": "$ts"}},
                             "tx": {"$sum": 1}, "fraud": {"$sum": "$is_fraud"}}}, {"$sort": {"_id": 1}}])
    st.plotly_chart(px.line(d, x="_id", y="fraud", title="Daily fraud count", labels={"_id": "date"}),
                    use_container_width=True)
    g = agg(TX, [{"$match": {"is_fraud": 1}}, {"$sample": {"size": 3000}},
                 {"$project": {"_id": 0, "lat": {"$arrayElemAt": ["$location.coordinates", 1]},
                               "lon": {"$arrayElemAt": ["$location.coordinates", 0]}}}])
    st.subheader("Where fraud happens (sample of 3,000 fraud transactions)")
    st.map(g)
    ctry = agg(TX, [{"$group": {"_id": "$country", "tx": {"$sum": 1}, "fraud": {"$sum": "$is_fraud"}}}])
    st.plotly_chart(px.bar(ctry, x="_id", y="fraud", title="Fraud by country", labels={"_id": "country"}),
                    use_container_width=True)

with tab3:
    mp = os.path.join(OUT_DIR, "metrics.json")
    if os.path.exists(mp):
        mt = json.load(open(mp))
        st.write(f"Best model: **{mt['best_model']}** (data read via: {mt['read_mode']})")
        st.dataframe(pd.DataFrame(mt["models"]).T, use_container_width=True)
    fp = os.path.join(OUT_DIR, "feature_importance.csv")
    if os.path.exists(fp):
        fi = pd.read_csv(fp).head(12).sort_values("importance")
        st.plotly_chart(px.bar(fi, x="importance", y="feature", orientation="h", title="Top feature importances"),
                        use_container_width=True)
    pr = pd.DataFrame(list(db[PRED].find({}, {"_id": 0}).sort("fraud_score", -1).limit(15)))
    st.subheader("Highest-risk transactions (fraud_predictions collection written by Spark)")
    st.dataframe(pr, use_container_width=True)

with tab4:
    cp = os.path.join(OUT_DIR, "compare_results.csv")
    if os.path.exists(cp):
        cr = pd.read_csv(cp)
        st.dataframe(cr, use_container_width=True)
        st.plotly_chart(px.bar(cr.melt(id_vars=["task", "fastest"]), x="task", y="value", color="variable",
                               barmode="group", title="Median runtime (ms)"), use_container_width=True)
    else:
        st.info("Run 07_compare_mongo_vs_spark.py first.")
