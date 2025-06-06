from airflow import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta, UTC
from pathlib import Path
import pandas as pd
import sys

sys.path.append('/opt/airflow/src')

from collector.binance_client import fetch_ohlcv
from formatter.ohlcv_formatter import format_ohlcv, save_to_parquet
from uploader.s3_uploader import upload_to_s3
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
                logger.info("No 1m data to aggregate")
                return

            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df.set_index('timestamp', inplace=True)

            ohlcv = df.resample(INTERVAL).agg({
                'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last',
                'volume': 'sum'
            }).dropna().reset_index()

            ohlcv = format_ohlcv(ohlcv, symbol, INTERVAL)
            ts = ohlcv['timestamp'].max()
            s3_key = ts.strftime('%Y-%m-%d_%H-%M')
            tmp_path = Path(f"/opt/airflow/tmp/{symbol}_{INTERVAL}_{s3_key}.parquet")
            save_to_parquet(ohlcv, tmp_path)
            upload_to_s3(ohlcv, symbol, INTERVAL, ts, s3_key)
            logger.info(f"{INTERVAL} uploaded to S3")
        except Exception as e:
            logger.error(f"{INTERVAL} task failed: {e}")

    for symbol in SYMBOLS:
        with TaskGroup(group_id=symbol):
            process(symbol)
