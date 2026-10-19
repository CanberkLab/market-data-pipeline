import os

from pyspark.sql import SparkSession

S3_BUCKET = os.getenv("S3_BUCKET", "market-data")
BRONZE_PATH = f"s3a://{S3_BUCKET}/bronze/ticks"
SILVER_PATH = f"s3a://{S3_BUCKET}/silver/ticks"
GOLD_PATH = f"s3a://{S3_BUCKET}/gold/ohlcv_1m"
QUARANTINE_PATH = f"s3a://{S3_BUCKET}/quarantine/ticks"
CHECKPOINT_PATH = f"s3a://{S3_BUCKET}/_checkpoints/bronze_ticks"

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "market.ticks")

JDBC_URL = os.getenv("DW_JDBC_URL", "jdbc:postgresql://postgres-dw:5432/market_dw")
JDBC_PROPS = {
    "user": os.getenv("DW_USER", "dw"),
    "password": os.getenv("DW_PASSWORD", "dw"),
    "driver": "org.postgresql.Driver",
}


def build_spark(app_name: str) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    )
    endpoint = os.getenv("S3_ENDPOINT")
    if endpoint:  # MinIO / lokal; gerçek AWS'de boş bırak
        builder = (
            builder.config("spark.hadoop.fs.s3a.endpoint", endpoint)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        )
    builder = builder.config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", "")) \
                     .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", ""))
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
