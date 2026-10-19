"""Saf (I/O içermeyen) dönüşüm fonksiyonları -> kolayca unit test edilir."""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

TICK_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("symbol", StringType()),
    StructField("price", DoubleType()),
    StructField("volume", LongType()),
    StructField("event_ts", StringType()),
    StructField("source", StringType()),
])


def apply_rules(df: DataFrame):
    """Cleansing. (valid_df, rejected_df) döner; reddedilenlere sebep yazılır."""
    df = (
        df.withColumn("symbol", F.upper(F.trim("symbol")))
        .withColumn("event_ts", F.to_timestamp("event_ts"))
        .withColumn(
            "reject_reason",
            F.when(F.col("event_id").isNull(), "corrupt_or_missing_event_id")
            .when(F.col("symbol").isNull() | (F.col("symbol") == ""), "missing_symbol")
            .when(F.col("event_ts").isNull(), "bad_timestamp")
            .when(F.col("price").isNull() | (F.col("price") <= 0), "invalid_price")
            .when(F.col("volume").isNull() | (F.col("volume") < 0), "invalid_volume"),
        )
    )
    valid = df.filter(F.col("reject_reason").isNull()).drop("reject_reason")
    rejected = df.filter(F.col("reject_reason").isNotNull())
    return valid, rejected


def deduplicate(df: DataFrame) -> DataFrame:
    """Aynı event_id'den sadece ilkini (en erken Kafka timestamp) tut."""
    from pyspark.sql.window import Window

    w = Window.partitionBy("event_id").orderBy(F.col("kafka_ts").asc_nulls_last()
                                              if "kafka_ts" in df.columns else F.col("event_ts"))
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def build_ohlcv_1m(df: DataFrame) -> DataFrame:
    """1 dakikalık OHLCV + VWAP barları."""
    return (
        df.groupBy("symbol", F.window("event_ts", "1 minute").alias("w"))
        .agg(
            F.min_by("price", "event_ts").alias("open"),
            F.max("price").alias("high"),
            F.min("price").alias("low"),
            F.max_by("price", "event_ts").alias("close"),
            F.sum("volume").alias("volume"),
            (F.sum(F.col("price") * F.col("volume")) / F.sum("volume")).alias("vwap"),
            F.count("*").alias("tick_count"),
        )
        .select(
            "symbol",
            F.col("w.start").alias("bar_start"),
            "open", "high", "low", "close", "volume",
            F.round("vwap", 4).alias("vwap"),
            "tick_count",
        )
    )
