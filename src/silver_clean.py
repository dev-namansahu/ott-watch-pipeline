#!/usr/bin/env python3
"""
PySpark cleaning job (ADF "Notebook/Spark" activity #1) - bronze/raw -> silver.

Daily incremental: processes one landing partition (dt=YYYY-MM-DD) per run date.
  * catalog / users: standardise text, dedupe on key, full refresh (small reference data)
  * events: type-safe parsing, standardise device_type, validate show + minutes, compute completion_pct,
            drop duplicate session_ids (inside the batch AND against the last N days already in silver),
            write partitioned silver/watch_events (dynamic partition overwrite => safe to re-run a day),
            quarantine bad rows in silver/rejects with a reason.
Invariant asserted for every run:  rows_read = rejected + duplicates_removed + rows_written
"""
import argparse
import glob
import json
import os
import sys
from datetime import date, datetime, timedelta, timezone

from pyspark.sql import SparkSession, Window, functions as F

GENRES = {"drama": "Drama", "comedy": "Comedy", "action": "Action", "thriller": "Thriller", "scifi": "Sci-Fi",
          "sciencefiction": "Sci-Fi", "romance": "Romance", "documentary": "Documentary", "horror": "Horror",
          "animation": "Animation"}
DEVICES = {"mobile": "MOBILE", "phone": "MOBILE", "android": "MOBILE", "ios": "MOBILE", "smartphone": "MOBILE",
           "tv": "TV", "smarttv": "TV", "television": "TV",
           "web": "WEB", "browser": "WEB", "desktop": "WEB", "pc": "WEB", "tablet": "TABLET"}
CONTENT = {"movie": "Movie", "film": "Movie", "series": "Series", "tvseries": "Series", "show": "Series"}
COUNTRY = {"india": "India", "in": "India", "ind": "India", "usa": "United States", "us": "United States",
           "unitedstates": "United States", "uk": "United Kingdom", "unitedkingdom": "United Kingdom",
           "canada": "Canada", "germany": "Germany", "australia": "Australia"}


def key(col):
    """lower-case, strip everything except letters/digits: ' Sci-Fi ' -> 'scifi'"""
    return F.regexp_replace(F.lower(F.trim(col)), r"[^a-z0-9]", "")


def standardize(col, mapping, null_value, default=None):
    m = F.create_map(*[x for k, v in mapping.items() for x in (F.lit(k), F.lit(v))])
    k = key(col)
    fallback = F.lit(default) if default is not None else F.initcap(F.trim(col))
    return F.when(col.isNull() | (k == ""), F.lit(null_value)).otherwise(F.coalesce(m[k], fallback))


def clean_reference(spark, inp, out):
    cat_raw = spark.read.option("header", True).csv(f"{inp}/raw/catalog/catalog.csv")
    cat = (cat_raw.select(
        F.trim("show_id").alias("show_id"),
        F.initcap(F.regexp_replace(F.trim("title"), r"\s+", " ")).alias("title"),
        standardize(F.col("genre"), GENRES, "Unknown", default="Other").alias("genre"),
        F.col("release_year").try_cast("int").alias("release_year"),
        F.col("runtime_minutes").try_cast("int").alias("runtime_minutes"),
        standardize(F.col("content_type"), CONTENT, "Unknown", default="Other").alias("content_type")))
    # duplicate show rows: keep the one that still has a runtime
    w = Window.partitionBy("show_id").orderBy(F.col("runtime_minutes").desc_nulls_last())
    cat = cat.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")
    cat.write.mode("overwrite").parquet(f"{out}/silver/catalog")

    users_raw = spark.read.option("header", True).csv(f"{inp}/raw/users/users.csv")
    users = (users_raw.select(
        F.trim("user_id").alias("user_id"),
        standardize(F.col("country"), COUNTRY, "Unknown").alias("country"),
        F.upper(F.trim("plan")).alias("plan"),
        F.coalesce(F.trim("age_group"), F.lit("Unknown")).alias("age_group"),
        F.to_date("signup_date").alias("signup_date")).dropDuplicates(["user_id"]))
    users.write.mode("overwrite").parquet(f"{out}/silver/users")
    os.makedirs(f"{out}/_runs", exist_ok=True)
    meta = {"catalog_raw_rows": cat_raw.count(), "catalog_silver_rows": cat.count(),
            "users_raw_rows": users_raw.count(), "users_silver_rows": users.count()}
    json.dump(meta, open(f"{out}/_runs/reference.json", "w"), indent=2)
    return cat


def process_day(spark, inp, out, run_date, catalog, lookback):
    path = f"{inp}/raw/events/dt={run_date}/events.csv"
    raw = spark.read.option("header", True).csv(path).cache()
    n_read = raw.count()

    ev = (raw.select(
        F.trim("session_id").alias("session_id"), F.trim("user_id").alias("user_id"), F.trim("show_id").alias("show_id"),
        standardize(F.col("device_type"), DEVICES, "UNKNOWN", default="UNKNOWN").alias("device_type"),
        F.col("watch_minutes").try_cast("int").alias("watch_minutes"),
        F.col("rating").try_cast("int").alias("rating_raw"),
        F.to_timestamp("event_ts").alias("event_ts"))
        .join(F.broadcast(catalog.select(F.col("show_id").alias("_cat_show"), "runtime_minutes")),
              F.col("show_id") == F.col("_cat_show"), "left"))

    reason = (F.when(F.col("_cat_show").isNull(), "INVALID_SHOW")
               .when(F.col("watch_minutes").isNull(), "NULL_MINUTES")
               .when(F.col("watch_minutes") < 0, "NEGATIVE_MINUTES"))
    ev = ev.withColumn("reject_reason", reason).cache()
    rejects = ev.filter("reject_reason IS NOT NULL")
    good = ev.filter("reject_reason IS NULL")

    # dedupe inside the batch, then against already-loaded days (late re-deliveries)
    w = Window.partitionBy("session_id").orderBy("event_ts")
    good = good.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")
    n_good_batch = good.count()
    silver_path = f"{out}/silver/watch_events"
    d = date.fromisoformat(run_date)
    if os.path.exists(silver_path):
        existing = (spark.read.parquet(silver_path)
                    .filter((F.col("event_date") >= F.lit(d - timedelta(days=lookback)))
                            & (F.col("event_date") < F.lit(d)))      # current day is overwritten, so not compared
                    .select("session_id"))
        good = good.join(existing, "session_id", "left_anti")

    silver = (good.withColumn("event_date", F.to_date("event_ts"))
              .withColumn("rating", F.when(F.col("rating_raw").between(1, 5), F.col("rating_raw")))
              .withColumn("runtime_missing_flag", F.col("runtime_minutes").isNull())
              .withColumn("completion_pct", F.when(F.col("runtime_minutes") > 0,
                          (F.least("watch_minutes", "runtime_minutes") * 100 / F.col("runtime_minutes")).cast("decimal(5,2)")))
              .withColumn("completed_flag", F.when(F.col("runtime_minutes") > 0,
                          F.col("watch_minutes") * 100 >= 90 * F.col("runtime_minutes")))
              .withColumn("over_100_flag", F.when(F.col("runtime_minutes") > 0, F.col("watch_minutes") > F.col("runtime_minutes")))
              .withColumn("run_date", F.lit(d))
              .select("session_id", "user_id", "show_id", "device_type", "watch_minutes", "rating", "event_ts",
                      "runtime_minutes", "completion_pct", "completed_flag", "over_100_flag", "runtime_missing_flag",
                      "run_date", "event_date"))
    silver = silver.cache()
    n_written = silver.count()
    silver.write.mode("overwrite").partitionBy("event_date").parquet(silver_path)

    rej_out = (rejects.select("session_id", "user_id", "show_id", "reject_reason")
               .withColumn("run_date", F.lit(d)))
    n_rej_by = {r["reject_reason"]: r["count"] for r in rej_out.groupBy("reject_reason").count().collect()}
    rej_out.write.mode("overwrite").partitionBy("run_date").parquet(f"{out}/silver/rejects")

    n_rej = sum(n_rej_by.values())
    dup_batch = ev.filter("reject_reason IS NULL").count() - n_good_batch
    dup_silver = n_good_batch - n_written
    m = {"run_date": run_date, "rows_read": n_read, "rejected": n_rej, "rejected_by_reason": n_rej_by,
         "duplicates_in_batch": dup_batch, "duplicates_vs_silver": dup_silver, "rows_written": n_written,
         "run_at": datetime.now(timezone.utc).isoformat()}
    assert n_read == n_rej + dup_batch + dup_silver + n_written, f"row-count invariant broken for {run_date}: {m}"
    json.dump(m, open(f"{out}/_runs/{run_date}.json", "w"), indent=2)
    raw.unpersist(); ev.unpersist(); silver.unpersist()
    return m


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="data/input")
    ap.add_argument("--output", default="data/lake")
    ap.add_argument("--run-date", help="process a single landing date (what ADF passes each day)")
    ap.add_argument("--lookback-days", type=int, default=3, help="late-arrival window for cross-batch dedupe")
    a = ap.parse_args()

    spark = (SparkSession.builder.appName("ott-silver-clean").master("local[*]")
             .config("spark.sql.shuffle.partitions", "8").config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
             .config("spark.ui.showConsoleProgress", "false").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")

    catalog = clean_reference(spark, a.input, a.output).cache()
    dates = [a.run_date] if a.run_date else sorted(os.path.basename(p)[3:] for p in glob.glob(f"{a.input}/raw/events/dt=*"))
    print(f"{'date':<12}{'read':>8}{'rejected':>10}{'dup_batch':>11}{'dup_late':>10}{'written':>9}")
    for d in dates:
        m = process_day(spark, a.input, a.output, d, catalog, a.lookback_days)
        print(f"{d:<12}{m['rows_read']:>8,}{m['rejected']:>10,}{m['duplicates_in_batch']:>11,}"
              f"{m['duplicates_vs_silver']:>10,}{m['rows_written']:>9,}")
    spark.stop()


if __name__ == "__main__":
    main()
