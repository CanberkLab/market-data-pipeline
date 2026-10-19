CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS market;
CREATE SCHEMA IF NOT EXISTS analytics;

-- Spark'ın yazdığı geçici tablo (her çalışmada truncate edilir)
CREATE TABLE IF NOT EXISTS staging.ohlcv_1m (
    symbol      TEXT,
    bar_start   TIMESTAMP,
    open        DOUBLE PRECISION,
    high        DOUBLE PRECISION,
    low         DOUBLE PRECISION,
    close       DOUBLE PRECISION,
    volume      BIGINT,
    vwap        DOUBLE PRECISION,
    tick_count  BIGINT
);

-- Kalıcı fact tablosu
CREATE TABLE IF NOT EXISTS market.ohlcv_1m (
    symbol      TEXT        NOT NULL,
    bar_start   TIMESTAMP   NOT NULL,
    open        DOUBLE PRECISION NOT NULL,
    high        DOUBLE PRECISION NOT NULL,
    low         DOUBLE PRECISION NOT NULL,
    close       DOUBLE PRECISION NOT NULL,
    volume      BIGINT      NOT NULL,
    vwap        DOUBLE PRECISION,
    tick_count  BIGINT      NOT NULL,
    loaded_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (symbol, bar_start)
);
CREATE INDEX IF NOT EXISTS idx_ohlcv_bar_start ON market.ohlcv_1m (bar_start);

CREATE TABLE IF NOT EXISTS market.dq_metrics (
    run_date        DATE,
    raw_count       BIGINT,
    rejected_count  BIGINT,
    duplicate_count BIGINT,
    silver_count    BIGINT,
    bar_count       BIGINT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
