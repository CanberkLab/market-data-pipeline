"""Bronze -> Silver (temiz, tekilleştirilmiş) -> Gold (1 dk OHLCV) -> Postgres staging.

Idempotent: aynı --date tekrar çalışırsa partition overwrite eder, staging truncate edilir.
"""
import argparse

from pyspark.sql import functions as F

from config import (BRONZE_PATH, GOLD_PATH, JDBC_PROPS, JDBC_URL, QUARANTINE_PATH,
                    SILVER_PATH, build_spark)
from transforms import apply_rules, build_ohlcv_1m, deduplicate


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True, help="YYYY-MM-DD")
    args = ap.parse_args()
    ds = args.date

    spark = build_spark(f"transform_{ds}")
    bronze = spark.read.parquet(BRONZE_PATH)

    # Bu güne ait olanlar + zaman damgası okunamayan (bozuk) kayıtlar
    ts = F.to_timestamp("event_ts")
    candidates = bronze.filter((F.to_date(ts) == F.lit(ds).cast("date")) | ts.isNull()).cache()

    valid, rejected = apply_rules(candidates)
    silver = deduplicate(valid).withColumn("trade_date", F.to_date("event_ts")).cache()

    raw_count, rejected_count = candidates.count(), rejected.count()
    valid_count, silver_count = valid.count(), silver.count()

    # Quarantine: reddedilenler sebep ile birlikte saklanır
    (rejected.withColumn("run_date", F.lit(ds).cast("date"))
     .write.mode("overwrite").partitionBy("run_date").parquet(QUARANTINE_PATH))

    # Silver
    silver.write.mode("overwrite").partitionBy("trade_date").parquet(SILVER_PATH)

    # Gold -> S3 + Postgres staging
    gold = build_ohlcv_1m(silver).cache()
    bar_count = gold.count()
    gold.withColumn("trade_date", F.to_date("bar_start")) \
        .write.mode("overwrite").partitionBy("trade_date").parquet(GOLD_PATH)
    (gold.write.mode("overwrite").option("truncate", "true")
     .jdbc(JDBC_URL, "staging.ohlcv_1m", properties=JDBC_PROPS))

    # Veri kalitesi metrikleri
    metrics = spark.createDataFrame(
        [(ds, raw_count, rejected_count, valid_count - silver_count, silver_count, bar_count)],
        "run_date string, raw_count long, rejected_count long, duplicate_count long, "
        "silver_count long, bar_count long",
    ).withColumn("run_date", F.col("run_date").cast("date"))
    metrics.write.mode("append").jdbc(JDBC_URL, "market.dq_metrics", properties=JDBC_PROPS)

    print(f"[{ds}] raw={raw_count} rejected={rejected_count} "
          f"duplicates={valid_count - silver_count} silver={silver_count} bars={bar_count}")


if __name__ == "__main__":
    main()
