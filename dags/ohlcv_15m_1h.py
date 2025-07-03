from airflow.utils.task_group import TaskGroup
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.exceptions import AirflowSkipException, AirflowFailException

from datetime import datetime, timedelta, timezone
import sys

sys.path.append("/opt/airflow/src")

UTC = timezone.utc

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

def build_dag(interval: str, cfg: dict):

    @dag(
        dag_id=cfg["dag_id"],
        start_date=datetime(2024, 1, 1, tzinfo=UTC),
        schedule=cfg["schedule"],
        catchup=False,
        default_args={
            "owner": "airflow",
            "retries": 1,
            "retry_delay": timedelta(minutes=1),
        },
        tags=["ohlcv", interval],
    )
    def _template():

        @task()
        def get_range() -> dict:
            """실행 시간에 따라 OHLCV 데이터의 시작과 끝 시간을 계산합니다."""
            ctx = get_current_context()
            logical_date = ctx["logical_date"].replace(second=0, microsecond=0)
            start = logical_date - timedelta(minutes=int(interval[:-1]))
            return {"start": start, "end": logical_date}

        @task()
        def fetch(symbol: str, start: datetime, end: datetime):
            """Binance API에서 OHLCV 데이터를 가져옵니다."""
            try:
                from collector.binance_client import fetch_ohlcv
                df = fetch_ohlcv(symbol, interval, start, end)
                if df.empty:
                    raise AirflowSkipException(f"{symbol}: empty df")
                return df
            except Exception as e:
                raise AirflowFailException(f"{symbol}: fetch fail - {e}")

        @task()
        def clean(df):
            """OHLCV 데이터의 형식을 정리합니다."""
            from formatter.ohlcv_formatter import clean_raw_ohlcv
            return clean_raw_ohlcv(df)

        @task()
        def format_df(df, symbol: str):
            """OHLCV 데이터를 최종 형식으로 변환합니다."""
            from formatter.ohlcv_formatter import format_ohlcv
            return format_ohlcv(df, symbol)

        @task()
        def upload(df, symbol: str, range_dict: dict):
            """정리된 OHLCV 데이터를 S3에 업로드합니다."""
            from uploader.s3_uploader import upload_to_s3
            ts = range_dict["end"]
            s3_key = f"{ts.strftime('%Y%m%d_%H%M')}.parquet"
            upload_to_s3(df, symbol, interval, ts, s3_key)

        @task()
        def load(df, symbol: str, range_dict: dict):
            """S3에 업로드된 OHLCV 데이터를 Snowflake에 로드합니다."""
            from uploader.snowflake_uploader import load_to_snowflake
            ts = range_dict["end"]
            s3_key = f"{ts.strftime('%Y%m%d_%H%M')}.parquet"
            load_to_snowflake(
                s3_path=f"{interval}/{symbol}/{s3_key}",
                table=cfg["table"]
            )

        def create_group(symbol: str, range_dict: dict) -> TaskGroup:
            """각 심볼에 대한 작업 그룹을 생성합니다."""
            with TaskGroup(group_id=f"{symbol}_group") as tg:
                raw       = fetch.override(task_id=f"{symbol}_fetch")(symbol, range_dict["start"], range_dict["end"])
                cleaned   = clean.override(task_id=f"{symbol}_clean")(raw)
                formatted = format_df.override(task_id=f"{symbol}_format")(cleaned, symbol)
                upload.override(task_id=f"{symbol}_upload")(formatted, symbol, range_dict)
                load.override(task_id=f"{symbol}_load")(formatted, symbol, range_dict)
            return tg

        range_dict = get_range()
        for sym in SYMBOLS:
            create_group(sym, range_dict)

    return _template()

globals()["ohlcv_15m"] = build_dag("15m", interval_config["15m"])
globals()["ohlcv_1h"]  = build_dag("1h", interval_config["1h"])
