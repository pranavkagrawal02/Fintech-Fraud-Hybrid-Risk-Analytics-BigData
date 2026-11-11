"""features.py - schema flattening + feature engineering shared by 05, 06 and 07 (PySpark)."""
from pyspark.sql import Window
from pyspark.sql import functions as F

CAT_COLS = ["type", "merchant_category", "device_os"]
NUM_COLS = ["amount", "log_amount", "hour_of_day", "is_night", "geo_distance_km", "country_mismatch",
            "dest_is_merchant", "device_tx_24h", "device_avg_amt_24h", "amount_to_device_avg",
            "dest_tx_24h"]
# NOTE: balance columns are deliberately NOT used - in PaySim, fraudulent transactions are cancelled so
# balances leak the label (Kaggle dataset notes say not to use them for fraud detection).


def flatten(df):
    """Distributed schema flattening: nested BSON structs/arrays -> flat columns."""
    return df.select(
        "tx_id",
        F.col("step").cast("long").alias("step"),
        F.col("ts").cast("timestamp").alias("ts"),
        "type", "amount", "merchant_category", "country",
        F.col("origin.account").alias("origin_account"),
        F.col("dest.account").alias("dest_account"),
        F.col("dest.is_merchant").cast("int").alias("dest_is_merchant"),
        F.col("device.id").alias("device_id"),
        F.col("device.os").alias("device_os"),
        F.col("origin.home_country").alias("home_country"),
        F.col("origin.home_lat").alias("home_lat"),
        F.col("origin.home_lon").alias("home_lon"),
        F.col("location.coordinates")[0].alias("tx_lon"),
        F.col("location.coordinates")[1].alias("tx_lat"),
        F.size("tags").alias("n_tags"),
        F.col("is_fraud").cast("int").alias("label"),
    )


def add_features(flat):
    """Windowed aggregations + engineered features."""
    lat1, lon1, lat2, lon2 = [F.radians(c) for c in ("home_lat", "home_lon", "tx_lat", "tx_lon")]
    a = F.sin((lat2 - lat1) / 2) ** 2 + F.cos(lat1) * F.cos(lat2) * F.sin((lon2 - lon1) / 2) ** 2
    w_dev = Window.partitionBy("device_id").orderBy("step").rangeBetween(-24, 0)   # previous 24 hours
    w_dst = Window.partitionBy("dest_account").orderBy("step").rangeBetween(-24, 0)
    return (flat
            .withColumn("log_amount", F.log1p("amount"))
            .withColumn("hour_of_day", F.hour("ts").cast("double"))
            .withColumn("is_night", (F.hour("ts") < 6).cast("int"))
            .withColumn("geo_distance_km", 2 * 6371 * F.asin(F.sqrt(a)))
            .withColumn("country_mismatch", (F.col("country") != F.col("home_country")).cast("int"))
            .withColumn("device_tx_24h", F.count("*").over(w_dev).cast("double"))
            .withColumn("device_avg_amt_24h", F.avg("amount").over(w_dev))
            .withColumn("amount_to_device_avg", F.col("amount") / (F.col("device_avg_amt_24h") + 1.0))
            .withColumn("dest_tx_24h", F.count("*").over(w_dst).cast("double")))


def feature_names(df, col="features"):
    """Recover human-readable feature names from the VectorAssembler metadata."""
    attrs = df.schema[col].metadata["ml_attr"]["attrs"]
    names = {}
    for group in attrs.values():
        for a in group:
            names[a["idx"]] = a["name"]
    return [names[i] for i in sorted(names)]
