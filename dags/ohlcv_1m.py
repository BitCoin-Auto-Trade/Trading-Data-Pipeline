from airflow import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import get_current_context

from datetime import datetime, timedelta, timezone
import sys

sys.path.append("/opt/airflow/src")

from collector.binance_client import fetch_ohlcv
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
    max_active_tasks=5,        
    max_active_runs=1,    
    concurrency=5,
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
    def fetch(symbol: str, start: datetime, end: datetime):
        return fetch_ohlcv(symbol, "1m", start, end)

    @task()
    def clean(df):
        return clean_raw_ohlcv(df)

    @task()
    def format_df(df, symbol: str):
        return format_ohlcv(df, symbol)

    @task()
    def upload_redis(df, symbol: str):
        upload_to_redis(df, symbol)

    def create_tasks(symbol: str) -> TaskGroup:
        with TaskGroup(group_id=f"{symbol}_group") as tg:
            range_dict = get_range.override(task_id=f"{symbol}_range")(symbol)
            df_raw = fetch.override(task_id=f"{symbol}_fetch")(symbol, range_dict["start"], range_dict["end"])
            df_clean = clean.override(task_id=f"{symbol}_clean")(df_raw)
            df_fmt = format_df.override(task_id=f"{symbol}_format")(df_clean, symbol)
            upload_redis.override(task_id=f"{symbol}_redis")(df_fmt, symbol)
        return tg

    for sym in SYMBOLS:
        create_tasks(sym)
