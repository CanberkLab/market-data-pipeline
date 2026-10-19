-- Analytics-ready, yeniden kullanılabilir veri setleri

CREATE OR REPLACE VIEW analytics.daily_summary AS
SELECT
    symbol,
    bar_start::date                                   AS trade_date,
    (array_agg(open  ORDER BY bar_start ASC))[1]      AS open,
    MAX(high)                                         AS high,
    MIN(low)                                          AS low,
    (array_agg(close ORDER BY bar_start DESC))[1]     AS close,
    SUM(volume)                                       AS volume,
    SUM(vwap * volume) / NULLIF(SUM(volume), 0)       AS vwap
FROM market.ohlcv_1m
GROUP BY symbol, bar_start::date;

CREATE OR REPLACE VIEW analytics.daily_returns AS
SELECT
    d.*,
    close / NULLIF(LAG(close) OVER (PARTITION BY symbol ORDER BY trade_date), 0) - 1 AS daily_return
FROM analytics.daily_summary d;

CREATE OR REPLACE VIEW analytics.moving_averages AS
SELECT
    symbol, bar_start, close, volume,
    AVG(close) OVER (PARTITION BY symbol ORDER BY bar_start ROWS BETWEEN 4  PRECEDING AND CURRENT ROW) AS sma_5,
    AVG(close) OVER (PARTITION BY symbol ORDER BY bar_start ROWS BETWEEN 14 PRECEDING AND CURRENT ROW) AS sma_15
FROM market.ohlcv_1m;

CREATE OR REPLACE VIEW analytics.intraday_volatility AS
SELECT
    symbol,
    bar_start::date AS trade_date,
    STDDEV_SAMP(close / NULLIF(open, 0) - 1) AS bar_return_stddev,
    (MAX(high) - MIN(low)) / NULLIF(MIN(low), 0) AS day_range_pct
FROM market.ohlcv_1m
GROUP BY symbol, bar_start::date;

CREATE OR REPLACE VIEW analytics.top_movers AS
SELECT trade_date, symbol, daily_return,
       RANK() OVER (PARTITION BY trade_date ORDER BY ABS(daily_return) DESC) AS rank
FROM analytics.daily_returns
WHERE daily_return IS NOT NULL;

CREATE OR REPLACE VIEW analytics.pipeline_health AS
SELECT run_date, raw_count, rejected_count, duplicate_count, silver_count, bar_count,
       ROUND(100.0 * rejected_count / NULLIF(raw_count, 0), 2) AS reject_pct,
       created_at
FROM market.dq_metrics
ORDER BY created_at DESC;
