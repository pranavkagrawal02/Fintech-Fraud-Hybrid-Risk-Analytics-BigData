# Troubleshooting

I could not run MongoDB or Spark where this package was built, so the scripts were syntax-checked and the data-preparation step was run for real, but the Mongo and Spark steps are untested on your cluster. Expect to fix one or two small environment issues. Most likely ones are below.

## "ServerSelectionTimeoutError" / cannot connect to MongoDB
MongoDB is not running on this node, or the address is different.
```
jps; ps aux | grep mongod
export MONGO_URI="mongodb://<host-where-mongod-runs>:27017"
```
If MongoDB needs a username/password:
`export MONGO_URI="mongodb://user:pass@host:27017/?authSource=admin"`
(Then run the mongo shell queries with your own connection string instead of `run_all.sh`.)

## `mongosh: command not found`
Try the older shell: `mongo "mongodb://localhost:27017/fraudlens" --file 04_mongo_aggregations.js`. Use whichever exists.

## "Unrecognized pipeline stage name: $setWindowFields" (or $count in a window)
MongoDB is older than 5.0. The script prints an error for that query and continues. In the report say that rolling analytics were done in Spark window functions because the lab's MongoDB version is ___ (<5.0). The other queries still work. Task T3 in step 7 also prints a warning for MongoDB and continues.

## Mongo-Spark connector cannot be downloaded (no internet)
`run_spark.sh` uses `--packages org.mongodb.spark:mongo-spark-connector_2.12:10.4.0`, which needs internet. Options:
1. Download the connector jar on your laptop from Maven Central (search "mongo-spark-connector_2.12 10.4.0"; if there is a file ending `-all.jar`, take that one because it bundles the driver dependencies), upload it, and run:
   `CONNECTOR_JAR=/path/to/mongo-spark-connector_2.12-10.4.0-all.jar bash run_spark.sh 05_pyspark_eda_features.py`
2. Do nothing: the scripts automatically fall back to pymongo -> Spark. Everything still runs; tick the "fallback" box in report section 7.1 and describe it honestly as a limitation.
3. Force the fallback and skip the connector attempt: `USE_CONNECTOR=0 bash run_spark.sh ...`

## "Failed to find data source: mongodb" or ClassNotFoundException
Same cause as above (connector missing). The scripts catch it and fall back. If your Spark was built with **Scala 2.13** (check `spark-submit --version`), use `CONNECTOR_PKG=org.mongodb.spark:mongo-spark-connector_2.13:10.4.0`.

## Spark 2.x in the lab
The scripts target Spark 3.x (`vector_to_array`, `weightCol` for GBT, connector 10.x). If `spark-submit --version` shows 2.x, tell your instructor/TA and use a newer Spark if one is installed (e.g. `/opt/spark3/bin/spark-submit`), or ask which version they expect.

## Out of memory / very slow
- Re-create a smaller sample on the laptop: `--rows 150000`.
- Give Spark more driver memory: `DRIVER_MEM=4g bash run_spark.sh 06_pyspark_ml_pipeline.py`.
- Keep `SPARK_MASTER=local[*]` (default). YARN is optional.

## `pip install` fails (no internet in lab)
Run `bash 00_check_env.sh` to see what is missing. pymongo and pandas are the only strictly required libraries for steps 3-7. matplotlib/seaborn are needed for step 7's chart and step 8b; plotly/streamlit only for the interactive dashboard. If pip cannot work, skip streamlit and generate the charts on your laptop (copy `outputs/*.csv` and `metrics.json` there and adapt `08b_dashboard_static.py`), or ask whether the lab offers a pip mirror.

## Streamlit page does not open
The lab may not expose port 8501 to your browser. Use `python3 08b_dashboard_static.py` instead; it needs no port.

## My files disappeared between sessions
Some labs reset the filesystem. Keep `fraudlens.zip` on your laptop, re-upload and unzip, and re-run `bash run_all.sh` (about 10-25 min).

## `03_load_to_mongodb.py` says duplicate / collection exists
It drops the collection by default. Use `--keep` only if you intentionally want to append.

## Different results every run
Spark uses seed 42 for the split and models, so numbers should be very close between runs but not always identical on different cluster sizes. Take your report numbers from ONE run (the one in your screenshots).
