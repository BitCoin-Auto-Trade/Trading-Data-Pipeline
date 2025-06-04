from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd

from src.collector.binance_client import fetch_ohlcv
from src.formatter.ohlcv_formatter import clean_ohlcv
from src.uploader.s3_uploader import upload_to_s3
from src.uploader.snowflake_uploader import load_to_snowflake

default_args = {    
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id = 'binance_pipeline_dag',
    default_args=default_args,
    description='바이낸스 15분 봉 데이터 수집 및 처리 파이프라인',
    schedule_interval='*/15 * * * *',  # 15분마다
    start_date=datetime(2023, 10, 1),
    catchup=False,
    tags=['binance', 'pipeline'],
) as dag:
    
    def task_fetch(**kwargs) -> pd.DataFrame:
        symbol = 'BTCUSDT'
        interval = '15m'
        end = datetime.utcnow()
        start = end - timedelta(minutes=15)
        df = fetch_ohlcv(symbol, interval, start, end)
        kwargs['ti'].xcom_push(key='raw_df', value=df.to_json())
        kwargs['ti'].xcom_push(key='symbol', value=symbol)
        kwargs['ti'].xcom_push(key='interval', value=interval)
        kwargs['ti'].xcom_push(key='timestamp', value=end.strftime('%Y-%m-%dT%H:%M:%S'))

    def task_format(**kwargs):
        raw_json = kwargs['ti'].xcom_pull(key='raw_df')
        symbol = kwargs['ti'].xcom_pull(key='symbol')
        interval = kwargs['ti'].xcom_pull(key='interval')
        df = pd.read_json(raw_json)
        formatted = clean_ohlcv(df, symbol, interval)
        kwargs['ti'].xcom_push(key='formatted_df', value=formatted.to_json())
    
    def task_upload_s3(**kwargs):
        df_json = kwargs['ti'].xcom_pull(key='formatted_df')
        symbol = kwargs['ti'].xcom_pull(key='symbol')
        interval = kwargs['ti'].xcom_pull(key='interval')
        timestamp_str = kwargs['ti'].xcom_pull(key='timestamp')
        timestamp = datetime.strptime(timestamp_str, '%Y-%m-%dT%H:%M:%S')
        df = pd.read_json(df_json)
        upload_to_s3(df, symbol, interval, timestamp)

    def task_upload_snowflake(**kwargs):
        symbol = kwargs['ti'].xcom_pull(key='symbol')
        interval = kwargs['ti'].xcom_pull(key='interval')
        timestamp_str = kwargs['ti'].xcom_pull(key='timestamp')
        dt = datetime.strptime(timestamp_str, '%Y-%m-%dT%H:%M:%S')
        s3_path = f"ohlcv/{interval}/{symbol}/{dt.strftime('%Y-%m-%d_%H')}.parquet"
        load_to_snowflake(s3_path, table='ohlcv_data')

    t1 = PythonOperator(
        task_id='fetch_binance_ohlcv',
        python_callable=task_fetch,
    )

    t2 = PythonOperator(
        task_id='format_ohlcv',
        python_callable=task_format,
    )

    t3 = PythonOperator(
        task_id='upload_to_s3',
        python_callable=task_upload_s3,
    )

    t4 = PythonOperator(
        task_id='upload_to_snowflake',
        python_callable=task_upload_snowflake,
    )

    # Task 연결
    t1 >> t2 >> t3 >> t4