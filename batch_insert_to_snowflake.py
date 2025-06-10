from datetime import datetime, timedelta, timezone

from collector.binance_client import fetch_ohlcv
from formatter.ohlcv_formatter import clean_raw_ohlcv, format_ohlcv
from uploader.s3_uploader import upload_to_s3
from uploader.snowflake_uploader import load_to_snowflake

UTC = timezone.utc
SYMBOLS = ["BTCUSDT", "ETHUSDT"]
INTERVALS = {
    "15m": timedelta(minutes=15),
    "1h": timedelta(hours=60),
}
LIMIT = 300

def run():
    now = datetime.now(tz=UTC).replace(second=0, microsecond=0)

    for interval, step in INTERVALS.items():
        for symbol in SYMBOLS:
            print(f">>> 최근 {LIMIT}개 백필 시작: {symbol} / {interval}")
            ts = now - (LIMIT * step)

            for _ in range(LIMIT):
                ts_next = ts + step
                try:
                    df = fetch_ohlcv(symbol, interval, ts, ts_next)
                    if df.empty:
                        print(f"{symbol} {interval} {ts} → 빈 데이터")
                        ts = ts_next
                        continue

                    df = clean_raw_ohlcv(df)
                    df = format_ohlcv(df, symbol)

                    if df.empty:
                        print(f"{symbol} {interval} {ts} → 정제 후 빈 데이터")
                        ts = ts_next
                        continue

                    s3_key = ts_next.strftime("%Y%m%d_%H%M.parquet")
                    upload_to_s3(df, symbol, interval, ts_next, s3_key)
                    load_to_snowflake(
                        s3_path=f"{interval}/{symbol}/{s3_key}",
                        table=f"ohlcv_{interval}"
                    )
                except Exception as e:
                    print(f"[ERROR] {symbol} {interval} {ts}: {e}")

                ts = ts_next

if __name__ == "__main__":
    run()
