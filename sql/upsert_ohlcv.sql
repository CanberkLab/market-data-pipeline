INSERT INTO market.ohlcv_1m (symbol, bar_start, open, high, low, close, volume, vwap, tick_count)
SELECT symbol, bar_start, open, high, low, close, volume, vwap, tick_count
FROM staging.ohlcv_1m
ON CONFLICT (symbol, bar_start) DO UPDATE SET
    open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low, close = EXCLUDED.close,
    volume = EXCLUDED.volume, vwap = EXCLUDED.vwap, tick_count = EXCLUDED.tick_count,
    loaded_at = now();
