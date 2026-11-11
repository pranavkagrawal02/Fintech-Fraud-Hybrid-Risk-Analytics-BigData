#!/bin/bash
# Runs the whole pipeline in order inside the lab. Takes roughly 10-25 minutes depending on data size.
# Usage:  bash run_all.sh
# Every step also writes a log into ../outputs/ so you can screenshot or copy results later.
cd "$(dirname "$0")"
mkdir -p ../outputs
if command -v mongosh >/dev/null; then MSH=mongosh; else MSH=mongo; fi
URI="${MONGO_URI:-mongodb://localhost:27017}"

echo "##### 0. environment check";   bash 00_check_env.sh | tee ../outputs/log_00_env.txt
echo "##### 3. load into MongoDB";   python3 03_load_to_mongodb.py | tee ../outputs/log_03_load.txt
echo "##### 4. Mongo aggregations";  $MSH "$URI/fraudlens" --quiet --file 04_mongo_aggregations.js | tee ../outputs/log_04_mongo_aggregations.txt
echo "##### 5. PySpark EDA";         bash run_spark.sh 05_pyspark_eda_features.py
echo "##### 6. MLlib pipeline";      bash run_spark.sh 06_pyspark_ml_pipeline.py
echo "##### 7. Mongo vs Spark";      bash run_spark.sh 07_compare_mongo_vs_spark.py
echo "##### 8b. static dashboard";   python3 08b_dashboard_static.py | tee ../outputs/log_08b_dashboard.txt
echo
echo "ALL DONE. Results are in ../outputs/ . For the interactive dashboard run:"
echo "  streamlit run 08_dashboard_streamlit.py --server.port 8501 --server.address 0.0.0.0"
