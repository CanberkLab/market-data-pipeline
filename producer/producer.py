"""Simüle piyasa tick verisi üretip Kafka'ya yollar.

Gerçekçi olması için bilerek kirli veri de üretir:
  - %2 duplicate (aynı event_id tekrar gönderilir)
  - %1 bozuk kayıt (null price, negatif volume, boş symbol, geçersiz JSON)
"""
import argparse
import json
import os
import random
from datetime import datetime, timedelta

from kafka import KafkaProducer

SYMBOLS = {
    "AAPL": 190.0, "MSFT": 410.0, "GOOGL": 150.0, "AMZN": 175.0,
    "TSLA": 240.0, "NVDA": 850.0, "JPM": 185.0, "BTC-USD": 65000.0,
}


def generate(date: str, count: int, seed: int = 42):
    """Yield edilen her eleman (key, bytes) çiftidir. event_id deterministik -> re-run idempotent."""
    rnd = random.Random(seed)
    prices = dict(SYMBOLS)
    start = datetime.fromisoformat(date).replace(hour=13, minute=30)
    span = 6.5 * 3600
    symbols = list(prices)
    for i in range(count):
        sym = rnd.choice(symbols)
        prices[sym] *= 1 + rnd.gauss(0, 0.0005)
        ts = start + timedelta(seconds=i * span / count)
        event = {
            "event_id": f"{date}-{i}",
            "symbol": sym,
            "price": round(prices[sym], 4),
            "volume": rnd.randint(1, 500),
            "event_ts": ts.strftime("%Y-%m-%d %H:%M:%S.") + f"{ts.microsecond // 1000:03d}",
            "source": "simulator",
        }
        r = rnd.random()
        if r < 0.01:
            bad = rnd.choice(["null_price", "neg_volume", "empty_symbol", "invalid_json"])
            if bad == "invalid_json":
                yield sym, b"{not-a-valid-json"
                continue
            if bad == "null_price":
                event["price"] = None
            elif bad == "neg_volume":
                event["volume"] = -5
            else:
                event["symbol"] = "  "
        payload = json.dumps(event).encode()
        yield sym, payload
        if r > 0.98:  # duplicate
            yield sym, payload


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--count", type=int, default=20000)
    ap.add_argument("--rate", type=float, default=0, help="saniyede mesaj (0 = sınırsız)")
    args = ap.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP", "localhost:29092"),
        key_serializer=lambda k: k.encode(),
        acks="all",
        linger_ms=20,
    )
    topic = os.getenv("KAFKA_TOPIC", "market.ticks")
    sent = 0
    for key, value in generate(args.date, args.count):
        producer.send(topic, key=key, value=value)
        sent += 1
        if args.rate:
            import time
            time.sleep(1 / args.rate)
    producer.flush()
    print(f"{sent} mesaj '{topic}' topic'ine gönderildi ({args.date}).")


if __name__ == "__main__":
    main()
