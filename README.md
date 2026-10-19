# End-to-End Financial Market Data Engineering Pipeline

**Stack:** Python · SQL · Apache Spark (PySpark) · Apache Airflow · Apache Kafka · Databricks · PostgreSQL · AWS S3 (MinIO ile lokal) · Docker

## Mimari

```
 producer.py ──► Kafka (market.ticks) ──► Spark Structured Streaming ──► S3 bronze (ham, parquet)
 (simüle tick,                                                               │
  dup + bozuk veri)                                                          ▼
                                                  Spark batch: cleansing + dedup + schema enforcement
                                                     ├─► S3 quarantine (reddedilenler + sebep)
                                                     ├─► S3 silver  (temiz tick, partition: trade_date)
                                                     └─► S3 gold    (1 dk OHLCV + VWAP)
                                                                             │ JDBC
                                                                             ▼
                                         Postgres staging ──(SQL upsert)──► market.ohlcv_1m
                                                                             │
                                                                             ▼
                                          analytics.* view'ları (daily_returns, moving_averages, ...)

 Airflow DAG: produce → stream_ingest → spark_transform → upsert → DQ check'ler
 Databricks : aynı silver/gold mantığı, Delta Lake + replaceWhere (databricks/ klasörü)
```

## Çalıştırma

```bash
make up          # Kafka, MinIO, Postgres x2, Airflow (ilk build birkaç dk sürer)
make trigger     # DAG'i tetikle  (veya http://localhost:8080  admin/admin)
make psql        # warehouse'a bağlan
```

Sonra:

```sql
SELECT * FROM analytics.pipeline_health;          -- kaç kayıt reddedildi / duplicate
SELECT * FROM analytics.daily_returns LIMIT 10;
SELECT * FROM analytics.moving_averages WHERE symbol='AAPL' ORDER BY bar_start DESC LIMIT 10;
```

MinIO konsolu: http://localhost:9001 (minioadmin/minioadmin) → `market-data` bucket'ında bronze/silver/gold/quarantine.

Unit testler: `pip install -r requirements.txt && make test` (Java 11/17 gerekir).

## Tasarım kararları (mülakatta anlatılabilir)

- **Bronze'da ham veri korunur:** bozuk JSON bile `raw_value` olarak saklanır; veri kaybı olmaz, reprocess edilebilir.
- **Exactly-once ingest:** Structured Streaming checkpoint + `availableNow` trigger (Airflow'dan batch gibi çalışır, streaming kodu aynı).
- **Cleansing kuralları kodda net:** `transforms.apply_rules` → reddedilen satırlar sebebiyle birlikte quarantine'e gider.
- **Deduplication:** `event_id` bazlı `row_number()`; producer deterministik id üretir, aynı gün tekrar çalışırsa sonuç değişmez.
- **Idempotency:** dinamik partition overwrite (S3), staging truncate + `ON CONFLICT DO UPDATE` (Postgres).
- **Staging → upsert deseni:** Spark sadece staging'e yazar, final tabloya yazma SQL ile atomik yapılır.
- **Data quality:** metrik tablosu (`market.dq_metrics`) + Airflow `SQLCheckOperator` (satır sayısı, OHLC tutarlılığı).
- **Test edilebilirlik:** I/O'suz saf Spark fonksiyonları + pytest.

## Prod'a taşırken

| Lokal | Prod |
|---|---|
| MinIO | `S3_ENDPOINT` boş bırak, IAM role / gerçek key |
| Spark `local[*]` (Airflow içinde) | Databricks job → `DatabricksSubmitRunOperator` ile `databricks/silver_gold_notebook.py` |
| Tek broker Kafka | MSK / Confluent Cloud, partition sayısı ↑ |
| Postgres | RDS / Redshift / Delta tablolar |

## Geliştirme fikirleri

Great Expectations ile DQ, Schema Registry + Avro, dbt ile `analytics` katmanı, Grafana/Metabase dashboard, gerçek veri için Binance/Alpaca WebSocket producer'ı, CI'da GitHub Actions ile pytest.
