from airflow import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta, UTC
import sys

sys.path.append('/opt/airflow/src')

from collector.binance_client import fetch_ohlcv
from formatter.ohlcv_formatter import format_ohlcv
from uploader.s3_uploader import upload_to_s3
from common.ohlcv_utils import SYMBOLS, now_range_1m, get_logger
from uploader.redis_uploader import upload_to_redis


def get_slot_index(ts: datetime) -> int:
    total_minutes = ts.hour * 60 + ts.minute
    return (total_minutes % 60) + 1  # 1~60

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

            df = format_ohlcv(df, symbol)
            ts = df['timestamp'].max()

            slot = get_slot_index(ts)
            s3_key = f"{slot}.parquet"

            upload_to_s3(df, symbol, '1m', ts, s3_key)
            upload_to_redis(df, symbol)

            logger.info(f"1m uploaded to S3: slot {slot}")
        except Exception as e:
            logger.error(f"1m task failed: {e}")

    for symbol in SYMBOLS:
        with TaskGroup(group_id=symbol):
            process(symbol)
