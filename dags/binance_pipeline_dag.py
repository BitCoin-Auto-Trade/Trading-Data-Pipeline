from datetime import datetime, timedelta, UTC
import pandas as pd
from airflow import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from pathlib import Path
import sys
import logging

sys.path.append('/opt/airflow/src')

from collector.binance_client import fetch_ohlcv
from formatter.ohlcv_formatter import format_ohlcv, save_to_parquet
from uploader.s3_uploader import upload_to_s3
from uploader.snowflake_uploader import load_to_snowflake

logger = logging.getLogger(__name__)
SYMBOLS = ['BTCUSDT', 'ETHUSDT']


def get_time_range(interval: str) -> tuple[datetime, datetime]:
    now = datetime.now(UTC).replace(second=0, microsecond=0)
    delta_min = {'1m': 1, '15m': 15, '1h': 60}[interval]
    end = now - timedelta(minutes=now.minute % delta_min)
    start = end - timedelta(minutes=delta_min)
    return start, end


def create_ohlcv_dag(interval: str, schedule: str):
    dag_id = f"ohlcv_{interval}_pipeline"

    with DAG(
        dag_id=dag_id,
        start_date=datetime(2024, 1, 1, tzinfo=UTC),
        schedule_interval=schedule,
        catchup=False,
        default_args={
            'owner': 'airflow',
            'retries': 1,
            'retry_delay': timedelta(minutes=2),
        },
        tags=['binance', 'ohlcv'],
        max_active_tasks=4,
    ) as dag:

        @task()
        def process(symbol: str):
            try:
                start, end = get_time_range(interval)
                df = fetch_ohlcv(symbol, interval, start, end)
                if df.empty:
                    logger.info(f"[{symbol}][{interval}] No data, skipping.")
                    return 'skip'

                df = format_ohlcv(df, symbol, interval)
                ts = pd.to_datetime(df['timestamp'].max())
                s3_key = ts.strftime('%Y-%m-%d_%H-%M')

                # 저장
                tmp_path = Path(f"/opt/airflow/tmp/{symbol}_{interval}_{s3_key}.parquet")
                save_to_parquet(df, tmp_path)

                # S3 업로드
                try:
                    upload_to_s3(df, symbol, interval, ts, s3_key)
                except Exception as e:
                    logger.error(f"[{symbol}][{interval}] S3 upload failed: {e}")
                    raise  # DAG 실패

                # Snowflake 적재 (에러 나도 skip)
                s3_path = f"{interval}/{symbol}/{s3_key}.parquet"
                try:
                    load_to_snowflake(s3_path, table='trading_db.public.ohlcv')
                except Exception as e:
                    logger.error(f"[{symbol}][{interval}] Snowflake 적재 실패: {e}")
                    # 실패해도 DAG는 계속

                return 'success'

            except Exception as e:
                logger.error(f"[{symbol}][{interval}] 전체 process 실패: {e}")
                return 'fail'

        for symbol in SYMBOLS:
            with TaskGroup(group_id=f'{symbol}') as tg:
                process(symbol)

        return dag


globals()['ohlcv_1m_pipeline'] = create_ohlcv_dag('1m', '* * * * *')
globals()['ohlcv_15m_pipeline'] = create_ohlcv_dag('15m', '*/15 * * * *')
globals()['ohlcv_1h_pipeline'] = create_ohlcv_dag('1h', '0 * * * *')
