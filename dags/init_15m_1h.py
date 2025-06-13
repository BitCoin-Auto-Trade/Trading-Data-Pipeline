from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta, UTC, timezone
import pandas as pd
import sys

sys.path.append("/opt/airflow/src")

from collector.binance_client import fetch_ohlcv
from formatter.ohlcv_formatter import clean_raw_ohlcv, format_ohlcv
from uploader.s3_uploader import upload_to_s3
from uploader.snowflake_uploader import load_to_snowflake

SYMBOLS = ["BTCUSDT", "ETHUSDT"]
UTC = timezone.utc
CONFIG = {
    "15m": {"count": 300, "table": "ohlcv_15m"},
    "1h": {"count": 300, "table": "ohlcv_1h"},
}


with DAG(
    dag_id="init_15m_1h",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["init", "ohlcv", "snowflake"],
) as dag:

    @task()
    def load_batch(symbol: str, interval: str):
        config = CONFIG[interval]
        count = config["count"]
        delta = {"15m": timedelta(minutes=15), "1h": timedelta(hours=1)}[interval]

        end = datetime.now(UTC).replace(second=0, microsecond=0)
        start = end - count * delta

        df = fetch_ohlcv(symbol, interval, start, end)
        if df.empty:
            print(f"[{symbol}][{interval}] 데이터 없음")
            return

        df = clean_raw_ohlcv(df)
        df = format_ohlcv(df, symbol)

        ts = pd.to_datetime(end)
        s3_key = f"{ts.strftime('%Y%m%d_%H%M')}_init.parquet"
        upload_to_s3(df, symbol, interval, ts, s3_key)

        load_to_snowflake(
            s3_path=f"{interval}/{symbol}/{s3_key}",
            table=config["table"]
        )

    for symbol in SYMBOLS:
        for interval in CONFIG:
            load_batch.override(task_id=f"{symbol}_{interval}")(symbol, interval)
