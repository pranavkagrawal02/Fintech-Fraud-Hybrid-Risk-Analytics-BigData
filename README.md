# FraudLens - Hybrid MongoDB + Spark Fraud Analytics

Real-time fraud and geospatial risk analytics for a payment processor, built on a hybrid architecture:
**MongoDB** as the operational document store and in-database analytics engine, **Apache Spark (PySpark MLlib)** for distributed feature engineering and model training, connected through the **Mongo-Spark Connector**, with scores written back to MongoDB and shown on a dashboard.

Course project: Big Data Systems (CC ZG522), BITS Pilani WILP - Assignment 2, Scenario 6 (Fintech), Track C.

## Architecture

```
Kaggle PaySim CSV --> sample + enrich (Python) --> nested JSONL
                                                     |
                                                     v
                         +-----------------  MongoDB  -----------------+
                         |  transactions (nested docs, 2dsphere idx)   |
                         |  devices ($group + $merge)                  |
                         |  Aggregation: $bucket $lookup $graphLookup  |
                         |               $setWindowFields geo explain  |
                         +----------------------+----------------------+
                                  Mongo-Spark Connector  |   ^  write-back
                                                         v   |
                              PySpark: flatten schema, window features,
                              MLlib RandomForest / GBT (class-weighted)
                                                         |
                                  fraud_predictions collection in MongoDB
                                                         |
                                                         v
                                 Dashboard (Streamlit / Matplotlib)
```

## Quick start

```bash
# 1. laptop: create the sample (needs pandas, numpy)
cd scripts
python 02_prepare_sample_and_enrich.py --input ../data/PS_20174392719_1491204439457_log.csv
#    or, without Kaggle:  python 02_prepare_sample_and_enrich.py --synthetic

# 2. cluster / machine with MongoDB 5.0+ and Spark 3.x
bash run_all.sh
```
Details: `docs/STEP_BY_STEP_PLAN.md`. What every file is for: `CONTENTS_AND_PURPOSE.md`.

## Dataset
PaySim - Synthetic Financial Datasets For Fraud Detection, https://www.kaggle.com/datasets/ealaxi/paysim1
(E. A. Lopez-Rojas, A. Elmir, S. Axelsson, EMSS 2016). Device, geo-location, merchant category and timestamp fields are synthetic enrichments added by `scripts/data_utils.py`.

## Requirements
MongoDB 5.0+, Spark 3.x with the `mongo-spark-connector_2.12` 10.x package, Python 3 with `pymongo`, `pandas`, `numpy`, `matplotlib`, `seaborn` (and optionally `plotly`, `streamlit`).

## Repository layout
`scripts/` pipeline code - `docs/` plan, checklist, troubleshooting - `report/` report template - `outputs/` generated results - `data/` input data.
