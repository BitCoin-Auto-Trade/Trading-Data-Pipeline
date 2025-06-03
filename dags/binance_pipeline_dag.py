import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import uuid
import io
import pandas as pd

from collector.binance_client import fetch_ohlcv
from formatter.ohlcv_formatter import format_ohlcv
from uploader.s3_uploader import upload_to_s3_raw_bytes
from uploader.snowflake_uploader import load_parquet_to_snowflake

# 환경변수에서 가져오기
SYMBOL = os.getenv("DAG_SYMBOLS", "BTCUSDT").split(",")[0]
INTERVAL = os.getenv("DAG_INTERVALS", "1h").split(",")[0]
BUCKET = os.getenv("AWS_S3_BUCKET", "lsj-bitcoin")
TABLE = f"{os.getenv('SNOWFLAKE_DATABASE')}.{os.getenv('SNOWFLAKE_SCHEMA')}.OHLCV"
STAGE = "ohlcv_stage"
FILE_FORMAT = "PARQUET_FORMAT"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="binance_pipeline_dag",
    default_args=default_args,
    description="Fetch OHLCV from Binance, format, upload to S3, then Snowflake",
    schedule_interval="0 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["binance", "etl"],
) as dag:

    def _fetch_ohlcv(**context):
        execution_date = context["execution_date"]
        start_time = execution_date
        end_time = execution_date + timedelta(hours=1)
        df = fetch_ohlcv(SYMBOL, INTERVAL, start_time, end_time)

        print(f"=== Fetched OHLCV ===\n{df}")
        context['ti'].xcom_push(key='raw_df', value=df.to_json())

    def _format_and_upload(**context):
        raw_json = context['ti'].xcom_pull(key='raw_df')
        df = pd.read_json(io.StringIO(raw_json))
        formatted_df = format_ohlcv(df)

        buf = io.BytesIO()
        formatted_df.to_parquet(buf, index=False)
        buf.seek(0)

        run_id = context['run_id'].replace(":", "_")
        filename = f"{SYMBOL}_{INTERVAL}_{run_id}_{uuid.uuid4().hex}.parquet"
        s3_key = f"ohlcv/{INTERVAL}/{SYMBOL}/{filename}"
        s3_path = f"s3://{BUCKET}/{s3_key}"

        upload_to_s3_raw_bytes(buf.read(), BUCKET, s3_key)
        context['ti'].xcom_push(key='s3_key', value=s3_key)

    def _load_to_snowflake(**context):
        s3_key = context['ti'].xcom_pull(key='s3_key')
        if not s3_key:
            raise ValueError("No S3 key found in XCom")

        load_parquet_to_snowflake(
            s3_key=s3_key,
            table_name=TABLE,
            stage_name=STAGE,
            file_format=FILE_FORMAT
        )

    t1 = PythonOperator(task_id="fetch_ohlcv", python_callable=_fetch_ohlcv)
    t2 = PythonOperator(task_id="format_and_upload", python_callable=_format_and_upload)
    t3 = PythonOperator(task_id="load_to_snowflake", python_callable=_load_to_snowflake)

    t1 >> t2 >> t3
