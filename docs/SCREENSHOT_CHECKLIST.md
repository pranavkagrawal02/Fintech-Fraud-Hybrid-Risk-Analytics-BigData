# Screenshot Checklist (26 figures)

The report template (`report/Assignment2_Report_Template.docx`) has one dashed box per figure with the same number.
Take the screenshots **in this order, in one lab session**, then paste them into the report offline.

Tips:
- Make the terminal font large and include the command line you typed in the screenshot.
- Figure 2 should be taken at the **start** (quota consumed ~0m) - it proves you used the lab environment.
- Figures 5 is taken on your laptop. Figures 1, 24-26 are diagrams/charts (outputs in `outputs/`).
- If a command prints a lot, scroll and take 2 screenshots; paste both in the same box.

| Fig | Report section | What | Command / source | Capture |
|---|---|---|---|---|
| 1 | 2 | Architecture diagram of the hybrid MongoDB + Spark solution | `Draw it yourself in draw.io / PowerPoint (boxes: Kaggle CSV -> prep script -> JSONL -> MongoDB -> Aggregation \| Spark via Mongo-Spark connector -> MLlib -> fraud_predictions in MongoDB -> Dashboard).` | Whole diagram, readable text. |
| 2 | 4 | BITS Prayogshala environment stack and node quota | `Browser: the Prayogshala 'Big Data Systems' page (the screen you already shared).` | Environment stack chips + 3 nodes with quota (do this BEFORE you start so consumed time is visible). |
| 3 | 4 | Environment check output (daemons, versions) | `cd scripts && bash 00_check_env.sh` | jps list, MongoDB version, Spark version, python libraries, hostname/date. |
| 4 | 4 | Project folder uploaded on the lab machine | `ls -R ~/fraudlens-mongo-spark-hybrid \| head -50   (and: ls -lh data/)` | Folder tree and the size of data/fraud_tx.jsonl.gz. |
| 5 | 5 | Data preparation / enrichment summary (laptop) | `python 02_prepare_sample_and_enrich.py --input ../data/PS_20174392719_1491204439457_log.csv` | Raw rows, frauds, rows kept, output file size. (Run on laptop - terminal screenshot.) |
| 6 | 5 | Loading documents into MongoDB | `cd scripts && python3 03_load_to_mongodb.py` | Insert count, indexes created, LOAD SUMMARY block at the end. |
| 7 | 5 | A sample nested transaction document | `mongosh fraudlens --eval 'printjson(db.transactions.findOne({is_fraud:1}))'` | Whole JSON document showing origin{}, dest{}, device{}, location{}, tags[]. |
| 8 | 6 | Q1 / Q2 - Fraud rate by transaction type and merchant category | `mongosh "mongodb://localhost:27017/fraudlens" --file 04_mongo_aggregations.js   (scroll to Q1, Q2)` | Q1 and Q2 headings with their output and runtime. |
| 9 | 6 | Q3 - $bucket amount bands | `same run as above (Q3 block)` | Q3 heading + bucket output. |
| 10 | 6 | Q4 - $setWindowFields rolling 24h device window | `same run as above (Q4a, Q4b)` | Q4a and Q4b output with runtime. |
| 11 | 6 | Q5 - $lookup join and $merge-built devices collection | `same run as above (Q5a, Q5b)` | Q5a and Q5b output. |
| 12 | 6 | Q6 - $graphLookup fraud money chains | `same run as above (Q6)` | Q6 output showing chain_length / chain_tx_ids. |
| 13 | 6 | Q7 / Q8 - Geospatial query and daily trend | `same run as above (Q7, Q7b, Q8)` | Q7, Q7b, Q8 output. |
| 14 | 6 | Q9 - Index scan vs collection scan (explain) | `same run as above (Q9 at the very end)` | Both lines: time, docsExamined, keysExamined. |
| 15 | 7 | PySpark reads nested documents from MongoDB | `cd scripts && bash run_spark.sh 05_pyspark_eda_features.py` | Line '>>> Read via Mongo-Spark Connector <<<' (or fallback line) + the nested printSchema(). |
| 16 | 7 | Flattened schema and window features | `same run (FLATTENED SCHEMA and SAMPLE WITH WINDOW FEATURES sections)` | Flattened printSchema and the table sorted by device_tx_24h. |
| 17 | 7 | EDA tables (by type, fraud vs normal, by hour) | `same run (EDA 1, EDA 2, EDA 3)` | The three result tables. |
| 18 | 8 | MLlib training results - Random Forest vs GBT | `cd scripts && bash run_spark.sh 06_pyspark_ml_pipeline.py` | JSON metrics of both models and the 'Best model' line. |
| 19 | 8 | Confusion matrices | `same run (Confusion matrix lines for each model)` | TN/FP/FN/TP for both models. |
| 20 | 8 | Feature importance | `same run (TOP 10 FEATURE IMPORTANCES) or plot in dashboard_3_model.png` | Top-10 list or the bar chart. |
| 21 | 9 | Predictions written back to MongoDB | `mongosh fraudlens --eval 'printjson(db.fraud_predictions.find({},{_id:0}).sort({fraud_score:-1}).limit(5).toArray()); print(db.fraud_predictions.countDocuments())'` | Top-5 highest scores + total count. |
| 22 | 10 | Mongo vs Spark timing results | `cd scripts && bash run_spark.sh 07_compare_mongo_vs_spark.py   (also paste outputs/compare_mongo_vs_spark.png)` | The results table (MONGO vs SPARK block) and/or the bar chart. |
| 23 | 10 | Spark UI / YARN UI showing the jobs | `While a Spark script runs open http://<node>:4040 (or the YARN ResourceManager UI if you used SPARK_MASTER=yarn)` | Jobs / Stages page with a job list and durations. |
| 24 | 11 | Dashboard - overview | `python3 08b_dashboard_static.py -> open outputs/dashboard_1_overview.png   (or the Streamlit 'Overview' tab)` | KPIs + type / category / amount-band charts. |
| 25 | 11 | Dashboard - geo and time | `outputs/dashboard_2_geo_time.png (or the Streamlit 'Geo & Time' tab)` | Daily trend + fraud location scatter/map. |
| 26 | 11 | Dashboard - model results | `outputs/dashboard_3_model.png (or the Streamlit 'ML Model' tab)` | Model metrics + feature importance. |
