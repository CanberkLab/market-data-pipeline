from transforms import apply_rules, build_ohlcv_1m, deduplicate

COLS = "event_id string, symbol string, price double, volume long, event_ts string, kafka_ts timestamp"


def make(spark, rows):
    return spark.createDataFrame(rows, COLS)


def test_apply_rules_splits_valid_and_rejected(spark):
    df = make(spark, [
        ("1", " aapl ", 10.0, 5, "2024-01-01 10:00:00", None),
        ("2", "AAPL", None, 5, "2024-01-01 10:00:00", None),
        ("3", "AAPL", 10.0, -1, "2024-01-01 10:00:00", None),
        ("4", "  ", 10.0, 1, "2024-01-01 10:00:00", None),
        ("5", "AAPL", 10.0, 1, "garbage", None),
        (None, None, None, None, None, None),
    ])
    valid, rejected = apply_rules(df)
    assert [r.symbol for r in valid.collect()] == ["AAPL"]  # trim + upper
    reasons = {r.reject_reason for r in rejected.collect()}
    assert reasons == {"invalid_price", "invalid_volume", "missing_symbol",
                       "bad_timestamp", "corrupt_or_missing_event_id"}


def test_deduplicate_keeps_one_per_event_id(spark):
    df = make(spark, [
        ("1", "AAPL", 10.0, 5, "2024-01-01 10:00:00", None),
        ("1", "AAPL", 10.0, 5, "2024-01-01 10:00:00", None),
        ("2", "AAPL", 11.0, 5, "2024-01-01 10:00:01", None),
    ])
    assert deduplicate(df).count() == 2


def test_ohlcv_bar(spark):
    df = make(spark, [
        ("1", "AAPL", 10.0, 100, "2024-01-01 10:00:05", None),
        ("2", "AAPL", 14.0, 100, "2024-01-01 10:00:20", None),
        ("3", "AAPL", 8.0, 200, "2024-01-01 10:00:40", None),
        ("4", "AAPL", 12.0, 100, "2024-01-01 10:00:55", None),
        ("5", "AAPL", 20.0, 50, "2024-01-01 10:01:10", None),
    ]).selectExpr("event_id", "symbol", "price", "volume",
                  "to_timestamp(event_ts) as event_ts")
    bars = {r.bar_start.minute: r for r in build_ohlcv_1m(df).collect()}
    b = bars[0]
    assert (b.open, b.high, b.low, b.close, b.volume, b.tick_count) == (10.0, 14.0, 8.0, 12.0, 500, 4)
    assert b.vwap == round((10 * 100 + 14 * 100 + 8 * 200 + 12 * 100) / 500, 4)
    assert bars[1].open == bars[1].close == 20.0
