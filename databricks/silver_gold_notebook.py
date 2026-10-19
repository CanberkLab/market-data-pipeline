# Databricks notebook source
# MAGIC %md
# MAGIC # Silver & Gold (Delta Lake)
# MAGIC Lokal `batch_transform.py` ile aynı iş mantığı; Databricks'te Auto Loader + Delta ile.
# MAGIC `transforms.py` bir Repo/Wheel olarak import edilir.

# COMMAND ----------
dbutils.widgets.text("run_date", "2024-01-01")
dbutils.widgets.text("bucket", "market-data")
run_date, bucket = dbutils.widgets.get("run_date"), dbutils.widgets.get("bucket")

import sys
sys.path.append("/Workspace/Repos/<kullanici>/market-data-pipeline/spark_jobs")  # repo yolunu düzenle
from pyspark.sql import functions as F
from transforms import apply_rules, deduplicate, build_ohlcv_1m

# COMMAND ----------
bronze = spark.read.parquet(f"s3://{bucket}/bronze/ticks")
ts = F.to_timestamp("event_ts")
candidates = bronze.filter((F.to_date(ts) == F.lit(run_date).cast("date")) | ts.isNull())

valid, rejected = apply_rules(candidates)
silver = deduplicate(valid).withColumn("trade_date", F.to_date("event_ts"))

# COMMAND ----------
# Delta: replaceWhere ile idempotent günlük yükleme
(silver.write.format("delta").mode("overwrite")
 .option("replaceWhere", f"trade_date = '{run_date}'")
 .partitionBy("trade_date").saveAsTable("market.silver_ticks"))

gold = build_ohlcv_1m(silver).withColumn("trade_date", F.to_date("bar_start"))
(gold.write.format("delta").mode("overwrite")
 .option("replaceWhere", f"trade_date = '{run_date}'")
 .partitionBy("trade_date").saveAsTable("market.gold_ohlcv_1m"))

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT symbol, trade_date, SUM(volume) AS volume, MAX(high) AS high, MIN(low) AS low
# MAGIC FROM market.gold_ohlcv_1m GROUP BY symbol, trade_date ORDER BY volume DESC
