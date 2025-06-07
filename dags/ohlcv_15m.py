from airflow import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta, UTC
from pathlib import Path
import pandas as pd
import sys

sys.path.append('/opt/airflow/src')

from collector.binance_client import fetch_ohlcv
from formatter.ohlcv_formatter import format_ohlcv
from uploader.s3_uploader import upload_to_s3
from uploader.snowflake_uploader import load_to_snowflake
from common.ohlcv_utils import SYMBOLS, get_time_range, get_logger

INTERVAL = '15m'
DELTA = 15

with DAG(
    dag_id="ohlcv_15m_pipeline",
    start_date=datetime(2024, 1, 1, tzinfo=UTC),
    schedule_interval="*/15 * * * *",
    catchup=False,
    default_args={
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
    },
    tags=['ohlcv', '15m'],
) as dag:

    @task()
    def process(symbol: str):
        logger = get_logger(f"15m-{symbol}")
        try:
            start, end = get_time_range(DELTA)
            df = fetch_ohlcv(symbol, '1m', start, end)
            if df.empty:
                logger.info("15m 데이터가 없습니다. 1m 데이터를 가져오지 못했습니다.")
                return

            df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
            df.set_index('timestamp', inplace=True)

            for col in ['open', 'high', 'low', 'close', 'volume']:
                df[col] = pd.to_numeric(df[col], errors='coerce')

            df = df.dropna(subset=['open', 'high', 'low', 'close', 'volume'])

            ohlcv = df.resample(INTERVAL).agg({
                'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last',
                'volume': 'sum'
            }).dropna().reset_index()

            if ohlcv.empty:
                logger.info("저장할 데이터가 없습니다.")
                return

            ohlcv = format_ohlcv(ohlcv, symbol)
            ts = pd.to_datetime(ohlcv['timestamp'].max(), errors='coerce')
            if pd.isnull(ts):
                logger.error("Aggregation 후 유효하지 않은 타임스탬프입니다.")
                return

            s3_key = "latest.parquet"
            upload_to_s3(ohlcv, symbol, INTERVAL, ts, s3_key)
            logger.info(f"{INTERVAL} uploaded to S3: {s3_key}")

            s3_path = f"{INTERVAL}/{symbol}/{s3_key}"
            table_name = f"ohlcv_{INTERVAL}"
            logger.info(f"Loading to Snowflake: {s3_path} -> {table_name}")
            load_to_snowflake(s3_path=s3_path, table=table_name)
            logger.info("Snowflake load 완료")

        except Exception as e:
            logger.error(f"{INTERVAL} task failed: {e}")

    for symbol in SYMBOLS:
        with TaskGroup(group_id=symbol):
            process(symbol)
