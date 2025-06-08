from airflow import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta, UTC
import sys

sys.path.append('/opt/airflow/src')

from collector.binance_client import fetch_ohlcv
from common.ohlcv_utils import process_ohlcv_task
from common.ohlcv_config import SYMBOLS, get_time_range
from utils.logger import get_logger

with DAG(
    dag_id="ohlcv_1h_pipeline",
    start_date=datetime(2024, 1, 1, tzinfo=UTC),
    schedule_interval="0 * * * *",
    catchup=False,
    default_args={
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
    },
    tags=['ohlcv', '1h'],
) as dag:

    @task()
    def process(symbol: str):
        logger = get_logger(f"1h-{symbol}")
        process_ohlcv_task(
            symbol=symbol,
            interval="1h",
            delta=60,
            logger=logger,
            fetch_func=fetch_ohlcv,
            get_range_func=get_time_range,
            upload_redis=False
        )

    for symbol in SYMBOLS:
        with TaskGroup(group_id=symbol):
            process(symbol)
