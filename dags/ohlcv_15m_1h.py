from airflow import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import get_current_context
from airflow.exceptions import AirflowSkipException, AirflowFailException

from datetime import datetime, timedelta, UTC
import sys

sys.path.append("/opt/airflow/src")

from collector.binance_client import fetch_ohlcv
from formatter.ohlcv_formatter import clean_raw_ohlcv, format_ohlcv
from uploader.s3_uploader import upload_to_s3
from uploader.snowflake_uploader import load_to_snowflake

interval_config = {
    "15m": {
        "dag_id": "ohlcv_15m",
        "schedule": "*/15 * * * *",
        "table": "ohlcv_15m",
    },
    "1h": {
        "dag_id": "ohlcv_1h",
        "schedule": "0 * * * *",
        "table": "ohlcv_1h",
    }
}

SYMBOLS = ["BTCUSDT", "ETHUSDT"]

def build_dag(interval: str, cfg: dict) -> DAG:
    with DAG(
        dag_id=cfg["dag_id"],
        start_date=datetime(2024, 1, 1, tzinfo=UTC),
        schedule_interval=cfg["schedule"],
        catchup=False,
        default_args={
            "owner": "airflow",
            "retries": 1,
            "retry_delay": timedelta(minutes=1),
        },
        tags=["ohlcv", interval],
        max_active_tasks=10,
        max_active_runs=2,
        concurrency=10,
    ) as dag:

        @task()
        def get_range() -> dict:
            """실행 시각 기준으로 start~end 시간 범위 구함"""
            ctx = get_current_context()
            logical_date = ctx["logical_date"].replace(second=0, microsecond=0)
            start = logical_date - timedelta(minutes=int(interval[:-1]))
            return {"start": start, "end": logical_date}

        @task()
        def fetch(symbol: str, start: datetime, end: datetime):
            """심볼별로 주어진 시간 구간의 OHLCV 데이터 수집"""
            try:
                df = fetch_ohlcv(symbol, interval, start, end)
                if df.empty:
                    raise AirflowSkipException(f"{symbol}: empty df")
                return df
            except Exception as e:
                raise AirflowFailException(f"{symbol}: fetch fail - {e}")

        @task()
        def clean(df):
            """수집한 원시 데이터 정제"""
            return clean_raw_ohlcv(df)

        @task()
        def format_df(df, symbol: str):
            """정제된 데이터에 심볼 붙이고 컬럼 포맷 통일"""
            return format_ohlcv(df, symbol)

        @task()
        def upload(df, symbol: str, range_dict: dict):
            ts = range_dict["end"]
            s3_key = f"{ts.strftime('%Y%m%d_%H%M')}.parquet"
            upload_to_s3(df, symbol, interval, ts, s3_key)

        @task()
        def load(df, symbol: str, range_dict: dict):
            ts = range_dict["end"]
            s3_key = f"{ts.strftime('%Y%m%d_%H%M')}.parquet"
            load_to_snowflake(
                s3_path=f"{interval}/{symbol}/{s3_key}",
                table=cfg["table"]
    )

        def create_group(symbol: str, range_dict: dict) -> TaskGroup:
            """하나의 심볼에 대해 전체 태스크(fetch → clean → format → upload → load) 묶는 TaskGroup 생성"""
            with TaskGroup(group_id=f"{symbol}_group") as tg:
                raw = fetch.override(task_id=f"{symbol}_fetch")(symbol, range_dict["start"], range_dict["end"])
                cleaned = clean.override(task_id=f"{symbol}_clean")(raw)
                formatted = format_df.override(task_id=f"{symbol}_format")(cleaned, symbol)
                upload.override(task_id=f"{symbol}_upload")(formatted, symbol, range_dict)
                load.override(task_id=f"{symbol}_load")(formatted, symbol, range_dict)
            return tg

        range_dict = get_range()
        for sym in SYMBOLS:
            create_group(sym, range_dict)

        return dag

globals()["collector_ohlcv_15m"] = build_dag("15m", interval_config["15m"])
globals()["collector_ohlcv_1h"] = build_dag("1h", interval_config["1h"])
