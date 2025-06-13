from airflow import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import get_current_context
from airflow.exceptions import AirflowSkipException, AirflowFailException

from datetime import datetime, timedelta, UTC
import sys
import pandas as pd

sys.path.append("/opt/airflow/src")

from uploader.snowflake_loader import fetch_ohlcv_from_snowflake
from formatter.indicator_calculator import calculate_indicators
from uploader.redis_uploader import upload_indicators_to_redis

SYMBOLS = ["BTCUSDT", "ETHUSDT"]

indicator_config = {
    "15m": {
        "dag_id": "redis_indicator_15m",
        "schedule": "*/15 * * * *",
        "table": "ohlcv_15m",
        "interval": "15m",
    },
    "1h": {
        "dag_id": "redis_indicator_1h",
        "schedule": "0 * * * *",
        "table": "ohlcv_1h",
        "interval": "1h",
    },
}


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
        tags=["indicator", "redis", interval],
    ) as dag:

        @task()
        def get_range() -> dict:
            ctx = get_current_context()
            logical_date = ctx["logical_date"].replace(second=0, microsecond=0)
            start = logical_date - timedelta(minutes=int(interval[:-1]) * 300)
            return {"start": start, "end": logical_date}

        @task()
        def fetch(symbol: str, start: datetime, end: datetime) -> pd.DataFrame:
            try:
                df = fetch_ohlcv_from_snowflake(symbol, interval, start, end)
                if df.empty:
                    raise AirflowSkipException(f"{symbol}: empty df")
                return df
            except Exception as e:
                raise AirflowFailException(f"{symbol}: fetch fail - {e}")

        @task()
        def calc(df: pd.DataFrame) -> pd.DataFrame:
            return calculate_indicators(df)

        @task()
        def upload(df: pd.DataFrame, symbol: str):
            upload_indicators_to_redis(df, symbol, interval)

        def create_group(symbol: str, range_dict: dict) -> TaskGroup:
            with TaskGroup(group_id=f"{symbol}_group") as tg:
                raw = fetch.override(task_id=f"{symbol}_fetch")(symbol, range_dict["start"], range_dict["end"])
                indicators = calc.override(task_id=f"{symbol}_calc")(raw)
                upload.override(task_id=f"{symbol}_upload")(indicators, symbol)
            return tg

        range_dict = get_range()
        for sym in SYMBOLS:
            create_group(sym, range_dict)

        return dag


globals()["redis_indicator_15m"] = build_dag("15m", indicator_config["15m"])
globals()["redis_indicator_1h"] = build_dag("1h", indicator_config["1h"])
