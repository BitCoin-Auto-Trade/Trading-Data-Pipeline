from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta, timezone, UTC
import pandas as pd
import sys

sys.path.append("/opt/airflow/src")

from collector.binance_client import fetch_ohlcv
from formatter.ohlcv_formatter import clean_raw_ohlcv, format_ohlcv
from uploader.redis_uploader import upload_to_redis

SYMBOLS = ["BTCUSDT", "ETHUSDT"]
UTC = timezone.utc

with DAG(
    dag_id="init_1m",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["init", "ohlcv", "redis", "1m"],
) as dag:

    @task()
    def init_redis(symbol: str):
        end = datetime.now(UTC).replace(second=0, microsecond=0)
        start = end - timedelta(minutes=60)

        df = fetch_ohlcv(symbol, "1m", start, end)
        if df.empty:
            print(f"[{symbol}] 데이터 없음")
            return

        df = clean_raw_ohlcv(df)
        df = format_ohlcv(df, symbol)
        upload_to_redis(df, symbol)

    for sym in SYMBOLS:
        init_redis.override(task_id=f"{sym}_init")(sym)
