#!/usr/bin/env python3
"""
PySpark gold job (ADF "Notebook/Spark" activity #2) - silver -> star schema + aggregates for SQL / Power BI.
Writes parquet to gold/ and single-file CSVs to gold_csv/. Also writes gold_summary.json (used by validate_results.py).
"""
import argparse
import glob
import json
import os
import shutil

from pyspark.sql import SparkSession, functions as F


def write(df, out, name):
    df.write.mode("overwrite").parquet(f"{out}/gold/{name}")
    tmp = f"{out}/gold_csv/_{name}"
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(tmp)
    shutil.move(glob.glob(f"{tmp}/part-*.csv")[0], f"{out}/gold_csv/{name}.csv")
    shutil.rmtree(tmp)
    print(f"  wrote gold/{name}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--output", default="data/lake")
    a = ap.parse_args()
    out = a.output
    spark = (SparkSession.builder.appName("ott-gold").master("local[*]").config("spark.sql.shuffle.partitions", "8")
             .config("spark.sql.session.timeZone", "UTC").config("spark.ui.showConsoleProgress", "false").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    os.makedirs(f"{out}/gold_csv", exist_ok=True)

    ev = spark.read.parquet(f"{out}/silver/watch_events").cache()
    shows = spark.read.parquet(f"{out}/silver/catalog")
    users = spark.read.parquet(f"{out}/silver/users")

    # ---- dimensions
    dim_show = shows
    dim_user = users
    dim_device = spark.createDataFrame([(1, "MOBILE"), (2, "TV"), (3, "WEB"), (4, "TABLET"), (5, "UNKNOWN")],
                                       ["device_key", "device_type"])
    lo, hi = ev.agg(F.min("event_date"), F.max("event_date")).first()
    dim_date = (spark.range(1).select(F.explode(F.sequence(F.lit(lo), F.lit(hi))).alias("date"))
                .select("date", F.year("date").alias("year"), F.month("date").alias("month"),
                        F.date_format("date", "EEEE").alias("day_name"), F.dayofweek("date").isin(1, 7).alias("is_weekend")))

    # ---- fact
    fact = (ev.join(F.broadcast(dim_device), "device_type", "left")
            .select("event_date", "session_id", "user_id", "show_id", "device_key", "device_type", "event_ts",
                    "watch_minutes", "rating", "completion_pct", "completed_flag", "over_100_flag", "runtime_missing_flag"))

    # ---- aggregates (completion rate = completed sessions / sessions whose runtime is known)
    def rate(c="completed_flag"):
        return F.round(100 * F.sum(F.col(c).cast("int")) / F.count(F.col(c)), 2)

    top_shows = (ev.groupBy("show_id").agg(
        F.count("*").alias("sessions"), F.sum("watch_minutes").alias("watch_minutes"),
        F.countDistinct("user_id").alias("unique_viewers"), rate().alias("completion_rate_pct"),
        F.round(F.avg("rating"), 2).alias("avg_rating"))
        .withColumn("watch_hours", F.round(F.col("watch_minutes") / 60, 2))
        .join(shows.select("show_id", "title", "genre", "content_type"), "show_id", "left")
        .orderBy(F.col("watch_hours").desc()))
    by_genre = (ev.join(shows.select("show_id", "genre"), "show_id").groupBy("genre").agg(
        F.count("*").alias("sessions"), F.sum("watch_minutes").alias("watch_minutes"),
        rate().alias("completion_rate_pct"), F.round(F.avg("completion_pct"), 2).alias("avg_completion_pct"),
        F.round(F.avg("rating"), 2).alias("avg_rating")).withColumn("watch_hours", F.round(F.col("watch_minutes") / 60, 2)))
    device_daily = (ev.groupBy("event_date", "device_type").agg(
        F.count("*").alias("sessions"), F.sum("watch_minutes").alias("watch_minutes"),
        F.countDistinct("user_id").alias("unique_viewers"))
        .withColumn("watch_hours", F.round(F.col("watch_minutes") / 60, 2)).orderBy("event_date", "device_type"))

    for name, df in [("dim_show", dim_show), ("dim_user", dim_user), ("dim_device", dim_device), ("dim_date", dim_date),
                     ("fact_watch_events", fact), ("agg_top_shows", top_shows), ("agg_completion_by_genre", by_genre),
                     ("agg_device_daily", device_daily)]:
        write(df, out, name)

    # ---- summary for validation
    j = ev.join(shows.select("show_id", "genre"), "show_id")
    t = ev.agg(F.count("*").alias("rows"), F.sum("watch_minutes").alias("minutes"),
               F.sum(F.col("completed_flag").cast("int")).alias("completed"),
               F.count("completed_flag").alias("known_runtime"),
               F.sum(F.col("over_100_flag").cast("int")).alias("over100"),
               F.sum(F.col("runtime_missing_flag").cast("int")).alias("rt_missing"),
               F.count("rating").alias("rating_count"), F.sum("rating").alias("rating_sum")).first()
    runs = [json.load(open(p)) for p in sorted(glob.glob(f"{out}/_runs/20*.json"))]
    rej = {}
    for r in runs:
        for k, v in r["rejected_by_reason"].items():
            rej[k] = rej.get(k, 0) + v
    ref = json.load(open(f"{out}/_runs/reference.json"))
    summary = {
        "days_processed": len(runs), "input_rows": sum(r["rows_read"] for r in runs), "rejects": rej,
        "duplicates_removed": sum(r["duplicates_in_batch"] + r["duplicates_vs_silver"] for r in runs),
        "silver_rows": t["rows"], "total_watch_minutes": t["minutes"],
        "by_device_minutes": {r.device_type: r["m"] for r in ev.groupBy("device_type").agg(F.sum("watch_minutes").alias("m")).collect()},
        "by_genre_minutes": {r.genre: r["m"] for r in j.groupBy("genre").agg(F.sum("watch_minutes").alias("m")).collect()},
        "completed_sessions": t["completed"], "sessions_with_known_runtime": t["known_runtime"],
        "over_100_sessions": t["over100"], "runtime_missing_sessions": t["rt_missing"],
        "rating_count": t["rating_count"], "rating_sum": t["rating_sum"],
        "catalog_raw_rows": ref["catalog_raw_rows"], "catalog_unique_shows": ref["catalog_silver_rows"], "users": ref["users_silver_rows"],
        "completion_rate_pct": round(100 * t["completed"] / t["known_runtime"], 2),
        "total_watch_hours": round(t["minutes"] / 60, 1), "avg_rating": round(t["rating_sum"] / t["rating_count"], 3)}
    json.dump(summary, open(f"{out}/gold_summary.json", "w"), indent=2)
    print(json.dumps(summary, indent=2))
    spark.stop()


if __name__ == "__main__":
    main()
