"""Kafka -> S3 (bronze) Structured Streaming.

Bronze = ham veri + Kafka metadata. Bozuk JSON'lar da raw_value olarak saklanır (veri kaybı yok).
  --once  : mevcut tüm offset'leri işle ve dur (Airflow için; checkpoint sayesinde exactly-once)
  (flag yok): sürekli çalışan streaming job
"""
import argparse

from pyspark.sql import functions as F

from config import BRONZE_PATH, CHECKPOINT_PATH, KAFKA_BOOTSTRAP, KAFKA_TOPIC, build_spark
from transforms import TICK_SCHEMA


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true")
    args = ap.parse_args()

    spark = build_spark("bronze_ingest")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    bronze = (
        raw.select(
            F.col("value").cast("string").alias("raw_value"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_ts"),
        )
        .withColumn("j", F.from_json("raw_value", TICK_SCHEMA))
        .select("j.*", "raw_value", "kafka_partition", "kafka_offset", "kafka_ts")
        .withColumn("ingest_date", F.to_date("kafka_ts"))
    )

    writer = (
        bronze.writeStream.format("parquet")
        .option("path", BRONZE_PATH)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .partitionBy("ingest_date")
        .outputMode("append")
    )
    writer = writer.trigger(availableNow=True) if args.once else writer.trigger(processingTime="30 seconds")
    query = writer.start()
    query.awaitTermination()


if __name__ == "__main__":
    main()
