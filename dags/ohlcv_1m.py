from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.sdk.python import get_current_context

from datetime import datetime, timedelta, timezone

SYMBOLS = ["BTCUSDT", "ETHUSDT"]
UTC = timezone.utc

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

@dag(
    dag_id="ohlcv_1m",
    start_date=datetime(2024, 1, 1, tzinfo=UTC),
    schedule="* * * * *",
    catchup=False,
    default_args=default_args,
    tags=["ohlcv", "1m"],
)
def ohlcv_1m():
    
    @task()
    def get_range(symbol: str) -> dict:
        """실행 시간에 따라 OHLCV 데이터의 시작과 끝 시간을 계산합니다."""
        ctx = get_current_context()
        logical_date = ctx["logical_date"].replace(second=0, microsecond=0)
        return {
            "start": logical_date,
            "end": logical_date + timedelta(minutes=1),
            "slot": logical_date.minute,
        }

    @task()
    def fetch(symbol: str, start: datetime, end: datetime):
        """Binance API에서 OHLCV 데이터를 가져옵니다."""
        from collector.binance_client import fetch_ohlcv
        return fetch_ohlcv(symbol, "1m", start, end)

    @task()
    def clean(df):
        """OHLCV 데이터의 형식을 정리합니다."""
        from formatter.ohlcv_formatter import clean_raw_ohlcv
        return clean_raw_ohlcv(df)

    @task()
    def format_df(df, symbol: str):
        """OHLCV 데이터를 포맷합니다."""
        from formatter.ohlcv_formatter import format_ohlcv
        return format_ohlcv(df, symbol)

    @task()
    def upload_redis(df, symbol: str):
        """OHLCV 데이터를 Redis에 업로드합니다."""
        from uploader.redis_uploader import upload_to_redis
        upload_to_redis(df, symbol)

    def create_tasks(symbol: str) -> TaskGroup:
        """각 심볼에 대한 태스크 그룹을 생성합니다."""
        with TaskGroup(group_id=f"{symbol}_group") as tg:
            range_dict = get_range.override(task_id=f"{symbol}_range")(symbol)
            df_raw = fetch.override(task_id=f"{symbol}_fetch")(symbol, range_dict["start"], range_dict["end"])
            df_clean = clean.override(task_id=f"{symbol}_clean")(df_raw)
            df_fmt = format_df.override(task_id=f"{symbol}_format")(df_clean, symbol)
            upload_redis.override(task_id=f"{symbol}_redis")(df_fmt, symbol)
        return tg

    for sym in SYMBOLS:
        create_tasks(sym)

dag = ohlcv_1m()
