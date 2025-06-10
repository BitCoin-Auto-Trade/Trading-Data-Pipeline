from airflow import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import get_current_context

from datetime import datetime, timedelta, timezone
import pandas as pd
import sys

sys.path.append("/opt/airflow/src")

from collector.binance_client import fetch_ohlcv
from uploader.s3_uploader import upload_to_s3
from uploader.redis_uploader import upload_to_redis
from formatter.ohlcv_formatter import clean_raw_ohlcv, format_ohlcv

SYMBOLS = ["BTCUSDT", "ETHUSDT"]
UTC = timezone.utc

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="ohlcv_1m",
    start_date=datetime(2024, 1, 1, tzinfo=UTC),
    schedule_interval="* * * * *",
    catchup=False,
    default_args=default_args,
    tags=["ohlcv", "1m"],
) as dag:

    @task()
    def get_range(symbol: str) -> dict:
        ctx = get_current_context()
        logical_date = ctx["logical_date"].replace(second=0, microsecond=0)
        return {
            "start": logical_date,
            "end": logical_date + timedelta(minutes=1),
            "slot": logical_date.minute,
        }

    @task()
    def fetch(symbol: str, start: datetime, end: datetime) -> pd.DataFrame:
        return fetch_ohlcv(symbol, "1m", start, end)

    @task()
    def clean(df: pd.DataFrame) -> pd.DataFrame:
        return clean_raw_ohlcv(df)

    @task()
    def format_df(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        return format_ohlcv(df, symbol)

    @task()
    def upload_s3(df: pd.DataFrame, symbol: str, slot: int, timestamp: datetime):
        key = f"{slot:02d}.parquet"
        upload_to_s3(df, symbol, "1m", timestamp, key)

    @task()
    def upload_redis(df: pd.DataFrame, symbol: str):
        upload_to_redis(df, symbol)

    def create_tasks(symbol: str) -> TaskGroup:
        with TaskGroup(group_id=f"{symbol}_group") as tg:
            range_dict = get_range.override(task_id=f"{symbol}_range")(symbol)
            df_raw = fetch.override(task_id=f"{symbol}_fetch")(symbol, range_dict["start"], range_dict["end"])
            df_clean = clean.override(task_id=f"{symbol}_clean")(df_raw)
            df_fmt = format_df.override(task_id=f"{symbol}_format")(df_clean, symbol)
            upload_s3.override(task_id=f"{symbol}_s3")(df_fmt, symbol, range_dict["slot"], range_dict["end"])
            upload_redis.override(task_id=f"{symbol}_redis")(df_fmt, symbol)
        return tg

    for sym in SYMBOLS:
        create_tasks(sym)
