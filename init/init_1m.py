from datetime import datetime, timedelta, timezone

from collector.binance_client import fetch_ohlcv
from formatter.ohlcv_formatter import clean_raw_ohlcv, format_ohlcv
from uploader.redis_uploader import upload_to_redis

SYMBOLS = ["BTCUSDT", "ETHUSDT"]
UTC = timezone.utc


def run():
    for symbol in SYMBOLS:
        end = datetime.now(UTC).replace(second=0, microsecond=0)
        start = end - timedelta(minutes=60)

        df = fetch_ohlcv(symbol, "1m", start, end)
        if df.empty:
            print(f"[{symbol}] 데이터 없음")
            continue

        df = clean_raw_ohlcv(df)
        df = format_ohlcv(df, symbol)
        upload_to_redis(df, symbol)


if __name__ == "__main__":
    run()
