import sys
from datetime import datetime, timedelta, UTC
import pandas as pd
from airflow import DAG
from airflow.decorators import task

# 경로 문제 방지
sys.path.append('/opt/airflow/src')  # Docker 환경 기준

from collector.binance_client import fetch_ohlcv
from formatter.ohlcv_formatter import format_ohlcv
from uploader.s3_uploader import upload_to_s3
from uploader.snowflake_uploader import load_to_snowflake

SYMBOL = 'BTCUSDT'
INTERVAL = '15m'


def get_time_range(interval: str) -> tuple[datetime, datetime]:
    now = datetime.now(UTC)
    delta = {'15m': 15, '1h': 60}.get(interval, 15)
    return now - timedelta(minutes=delta), now


with DAG(
    dag_id='binance_ohlcv_pipeline',
    start_date=datetime.now(UTC) - timedelta(minutes=15),
    schedule_interval='*/15 * * * *',
    catchup=False,
    default_args={
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    },
    description='바이낸스 OHLCV 수집 및 적재 파이프라인',
    tags=['binance', 'ohlcv'],
) as dag:

    @task()
    def fetch() -> str:
        start, end = get_time_range(INTERVAL)
        df = fetch_ohlcv(SYMBOL, INTERVAL, start, end)
        return df.to_json()

    @task()
    def format(raw_json: str) -> str:
        df = pd.read_json(raw_json)
        cleaned = format_ohlcv(df)
        return cleaned.to_json()

    @task()
    def upload_s3(cleaned_json: str) -> str:
        df = pd.read_json(cleaned_json)
        timestamp = df['timestamp'].max()
        ts = pd.to_datetime(timestamp)
        upload_to_s3(df, SYMBOL, INTERVAL, ts)
        return ts.strftime('%Y-%m-%d_%H')

    @task()
    def upload_snowflake(s3_key_ts: str):
        s3_path = f"ohlcv/{INTERVAL}/{SYMBOL}/{s3_key_ts}.parquet"
        load_to_snowflake(s3_path, table='ohlcv')

    # DAG 연결
    raw = fetch()
    formatted = format(raw)
    s3_key = upload_s3(formatted)
    upload_snowflake(s3_key)
