from airflow import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta, UTC
from pathlib import Path
import sys

sys.path.append('/opt/airflow/src')

from collector.binance_client import fetch_ohlcv
from formatter.ohlcv_formatter import format_ohlcv, save_to_parquet
from uploader.s3_uploader import upload_to_s3
from common.ohlcv_utils import SYMBOLS, now_range_1m, get_logger

with DAG(
    dag_id="ohlcv_1m_pipeline",
    start_date=datetime(2024, 1, 1, tzinfo=UTC),
    schedule_interval="* * * * *",
    catchup=False,
    default_args={
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
    },
    tags=['ohlcv', '1m'],
) as dag:

    @task()
    def process(symbol: str):
        logger = get_logger(f"1m-{symbol}")
        try:
            start, end = now_range_1m()
            df = fetch_ohlcv(symbol, '1m', start, end)
            if df.empty:
                logger.info("No 1m data. Skip.")
                return

            df = format_ohlcv(df, symbol, '1m')
            ts = df['timestamp'].max()
            s3_key = ts.strftime('%Y-%m-%d_%H-%M')
            tmp_path = Path(f"/opt/airflow/tmp/{symbol}_1m_{s3_key}.parquet")
            save_to_parquet(df, tmp_path)
            upload_to_s3(df, symbol, '1m', ts, s3_key)
            logger.info("1m uploaded to S3")
        except Exception as e:
            logger.error(f"1m task failed: {e}")

    for symbol in SYMBOLS:
        with TaskGroup(group_id=symbol):
            process(symbol)
