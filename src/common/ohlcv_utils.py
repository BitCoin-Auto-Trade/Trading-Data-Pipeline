from datetime import datetime
import pandas as pd
from uploader.s3_uploader import upload_to_s3
from uploader.snowflake_uploader import load_to_snowflake
from uploader.redis_uploader import upload_to_redis
from formatter.ohlcv_formatter import clean_raw_ohlcv, resample_ohlcv, format_ohlcv

def get_slot_index(ts: datetime) -> int:
    return (ts.hour * 60 + ts.minute) % 60 + 1

def get_s3_key(ts: datetime, interval: str) -> str:
    if interval == "1m":
        return f"{get_slot_index(ts)}.parquet"
    return ts.strftime("%Y%m%d_%H%M") + ".parquet"

def process_ohlcv_task(
    symbol: str,
    interval: str,
    delta: int,
    logger,
    fetch_func,
    get_range_func,
    upload_redis: bool = False,
) -> None:
    start, end = get_range_func(delta)
    df = fetch_func(symbol, '1m', start, end)
    if df.empty:
        logger.info(f"{interval} 데이터가 없습니다.")
        return

    df = clean_raw_ohlcv(df)
    ohlcv = df if interval == '1m' else resample_ohlcv(df, interval)
    if ohlcv.empty:
        logger.info("집계 결과 데이터 없음.")
        return

    ohlcv = format_ohlcv(ohlcv, symbol)
    ts = pd.to_datetime(ohlcv['timestamp'].max(), errors='coerce')
    if pd.isnull(ts):
        logger.error("timestamp invalid")
        return

    s3_key = get_s3_key(ts, interval)
    upload_to_s3(ohlcv, symbol, interval, ts, s3_key)

    if upload_redis:
        upload_to_redis(ohlcv, symbol)

    if interval != '1m':
        s3_path = f"{interval}/{symbol}/{s3_key}"
        load_to_snowflake(s3_path=s3_path, table=f"ohlcv_{interval}")
