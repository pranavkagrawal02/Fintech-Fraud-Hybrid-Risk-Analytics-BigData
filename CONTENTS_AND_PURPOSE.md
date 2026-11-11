# What Is In This Folder and Why

Project name: **fraudlens-mongo-spark-hybrid** (use this as your GitHub repository name).
Goal: finish BITS Assignment 2 (Big Data Systems) with Scenario 6 (Fintech fraud), Track C (hybrid MongoDB + PySpark), doing the least possible work inside the lab.

## Start here
1. Read `docs/STEP_BY_STEP_PLAN.md` (or `.txt`).
2. Do the laptop steps, zip the folder, upload to the lab.
3. Run the scripts, take the screenshots listed in `docs/SCREENSHOT_CHECKLIST.md`.
4. Paste them into `report/Assignment2_Report_Template.docx`.

## Files and purpose

| Item | Purpose | Where you use it |
|---|---|---|
| `README.md` | Repository front page for GitHub (overview, quick start, structure). | GitHub |
| `CONTENTS_AND_PURPOSE.md` / `.txt` | This file: what every item is for. | Anywhere |
| `requirements.txt` | Python libraries needed (pymongo, pandas, numpy, matplotlib, seaborn, plotly, streamlit). Spark is NOT in it on purpose. | Lab / laptop |
| `.gitignore` | Keeps the 470 MB CSV, data sample and outputs out of Git. | GitHub |
| **docs/** | | |
| `docs/STEP_BY_STEP_PLAN.md` / `.txt` | The full plan: schedule, laptop steps, lab steps with exact commands, report steps, GitHub push, final checklist, mapping to learning outcomes. | Read first |
| `docs/DATASET_LINKS.md` / `.txt` | Kaggle download link for PaySim, file details, paper to cite, what to do if you cannot download. | Laptop |
| `docs/SCREENSHOT_CHECKLIST.md` / `.txt` | The 26 screenshots: figure number, command to run, what to capture. Matches the report boxes 1:1 so you can capture everything in one pass. | Lab |
| `docs/TROUBLESHOOTING.md` / `.txt` | Fixes for likely problems: Mongo connection, old MongoDB version, connector download, Spark version, memory, pip, dashboard port. | When something fails |
| **scripts/** (numbers = order of execution) | | |
| `scripts/00_check_env.sh` | Shows daemons (jps), MongoDB/Spark/Python versions, missing libraries, disk. First command in the lab. | Lab (Figure 3) |
| `scripts/01_setup_python_env.sh` | Installs missing Python libraries with pip --user. | Lab, only if needed |
| `scripts/02_prepare_sample_and_enrich.py` | Takes the raw PaySim CSV, keeps all fraud + a random sample, adds synthetic device / geo / merchant / timestamp fields, writes nested JSONL.gz. `--synthetic` generates the data if you cannot download. | **Laptop** (Figure 5) |
| `scripts/data_utils.py` | Helper code for step 02 (enrichment + synthetic generator). | Imported by step 02 |
| `scripts/common.py` | Shared settings (Mongo URI, DB name) and helpers to read/write MongoDB from Spark with connector + automatic pymongo fallback. | Imported by all scripts |
| `scripts/03_load_to_mongodb.py` | Loads documents into MongoDB, creates indexes (including 2dsphere), builds the `devices` collection with $group + $merge. | Lab (Figures 6-7) |
| `scripts/04_mongo_aggregations.js` | Nine MongoDB analytics: $group, $bucket, $setWindowFields, $lookup, $graphLookup, geospatial, trend, explain index vs scan. This is the Track A part. | Lab (Figures 8-14) |
| `scripts/features.py` | PySpark schema flattening and feature engineering (window functions, haversine distance). | Imported by 05, 06, 07 |
| `scripts/05_pyspark_eda_features.py` | Reads from MongoDB into Spark, shows nested vs flat schema, windowed features, EDA tables. | Lab (Figures 15-17) |
| `scripts/06_pyspark_ml_pipeline.py` | MLlib Random Forest and GBT with class weights; metrics, confusion matrix, feature importance; writes fraud scores back to MongoDB (`fraud_predictions`). Track B + C. | Lab (Figures 18-21) |
| `scripts/07_compare_mongo_vs_spark.py` | Times 3 tasks in MongoDB vs Spark (cached and end-to-end) and plots the result. This answers the assignment's central question: where should compute reside? | Lab (Figure 22) |
| `scripts/08_dashboard_streamlit.py` | Interactive dashboard (Streamlit + Plotly) reading live from MongoDB. | Lab, if port is reachable |
| `scripts/08b_dashboard_static.py` | Static dashboard PNGs (matplotlib/seaborn). Always works, no browser needed. | Lab (Figures 24-26) |
| `scripts/run_spark.sh` | Wrapper for spark-submit with the Mongo-Spark connector, local or YARN, optional local jar. | Lab |
| `scripts/run_all.sh` | Runs steps 0, 3, 4, 5, 6, 7, 8b in order and saves logs in `outputs/`. | Lab (one-shot) |
| **data/** | | |
| `data/README.txt` | Tells you which files go here (raw CSV on laptop; the small `fraud_tx.jsonl.gz` is what you upload). | Laptop |
| **outputs/** | Empty on purpose. Logs, CSVs, `metrics.json` and dashboard PNGs are created here by the scripts. Download this folder after the lab session. | Lab |
| **report/** | | |
| `report/Assignment2_Report_Template.docx` | The report: cover page, 13 sections, appendix, pre-written draft text, code snippets, empty tables for your numbers, 26 dashed boxes for screenshots with the command to run printed in each box. | Laptop |

## Things to know about this package
- Tested here: the data-preparation script (it ran and produced valid nested documents), Python and shell syntax, JavaScript syntax, and the Word report rendering.
- Not tested here: anything that needs your MongoDB or Spark cluster (steps 03 to 08). I did not have access to a cluster. Expect to adjust small things (connection URI, connector jar); see `docs/TROUBLESHOOTING.md`.
- The device, geography, merchant-category and timestamp fields are SYNTHETIC enrichment. Fraud rows are deliberately more likely to use shared devices and unusual locations. Model scores will therefore look better than on real data - say this in your report (the template already has a place for it).
- The report text is a draft. The numbers, observations and discussion must come from your own runs.
- I do not know the grading rubric. The plan maps the work to the learning outcomes listed in the assignment PDF.
