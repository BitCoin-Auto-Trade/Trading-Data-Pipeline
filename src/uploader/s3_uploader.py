import logging
from datetime import datetime

logger = logging.getLogger(__name__)

def get_s3_bucket() -> str:
    """환경변수에서 S3 버킷 이름을 가져옴"""
    import os
    bucket = os.getenv("AWS_S3_BUCKET")
    if not bucket:
        raise RuntimeError("환경변수 AWS_S3_BUCKET가 설정되지 않았음")
    return bucket


def upload_parquet_bytes(raw_bytes: bytes, bucket: str, s3_key: str, max_attempts: int = 5) -> None:
    """S3에 Parquet 파일을 바이트 스트림으로 업로드"""
    import boto3
    from botocore.exceptions import BotoCoreError, ClientError

    s3 = boto3.client("s3")

    for attempt in range(1, max_attempts + 1):
        try:
            s3.put_object(Bucket=bucket, Key=s3_key, Body=raw_bytes)
            logger.info(f"Uploaded to s3://{bucket}/{s3_key}")
            return
        except (BotoCoreError, ClientError) as e:
            level = logging.WARNING if attempt == max_attempts else logging.INFO
            logger.log(level, f"[{attempt}/{max_attempts}] S3 upload failed: {e}")
            if attempt == max_attempts:
                raise


def upload_to_s3(df, symbol: str, interval: str, ts: datetime, s3_key: str) -> None:
    """S3에 Parquet 파일로 업로드"""
    if df is None or df.empty:
        logger.warning(f"[upload_to_s3] Empty DataFrame for {symbol}-{interval}, skip upload")
        return

    import pandas as pd
    from io import BytesIO

    required_columns = ["timestamp", "open", "high", "low", "close", "volume", "symbol"]
    for col in required_columns:
        if col not in df.columns:
            raise ValueError(f"Missing required column: {col}")

    df = df[required_columns].copy()
    df = df.sort_values("timestamp")
    df = df.drop_duplicates(subset=["timestamp", "symbol"], keep="last")

    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df["symbol"] = df["symbol"].astype(str)
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = df[col].astype(float)

    buffer = BytesIO()
    df.to_parquet(buffer, index=False, coerce_timestamps="us")
    buffer.seek(0)

    bucket = get_s3_bucket()
    full_s3_key = f"ohlcv/{interval}/{symbol}/{s3_key}"
    upload_parquet_bytes(buffer.read(), bucket, full_s3_key)
