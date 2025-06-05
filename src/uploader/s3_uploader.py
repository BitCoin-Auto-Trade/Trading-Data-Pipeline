import logging
import boto3
import pandas as pd
import os
from datetime import datetime
from io import BytesIO
from botocore.exceptions import BotoCoreError, ClientError

logger = logging.getLogger(__name__)
s3 = boto3.client("s3")


def upload_parquet_bytes(raw_bytes: bytes, bucket: str, s3_key: str) -> None:
    try:
        s3.put_object(Bucket=bucket, Key=s3_key, Body=raw_bytes)
        logger.info(f"Uploaded to s3://{bucket}/{s3_key}")
    except (BotoCoreError, ClientError) as e:
        logger.error(f"S3 upload failed: {e}")
        raise


def upload_to_s3(df: pd.DataFrame, symbol: str, interval: str, timestamp: datetime, s3_key: str) -> None:
    df = df[[
        "timestamp", "open", "high", "low", "close", "volume", "symbol", "interval"
    ]]

    df["timestamp"] = pd.to_datetime(df["timestamp"]).astype(str)
    df["open"] = df["open"].astype(float)
    df["high"] = df["high"].astype(float)
    df["low"] = df["low"].astype(float)
    df["close"] = df["close"].astype(float)
    df["volume"] = df["volume"].astype(float)
    df["symbol"] = df["symbol"].astype(str)
    df["interval"] = df["interval"].astype(str)

    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)

    s3_key = f"ohlcv/{interval}/{symbol}/{s3_key}.parquet"
    bucket = os.getenv("AWS_S3_BUCKET")

    if not bucket:
        raise RuntimeError("AWS_S3_BUCKET 환경변수가 설정되어 있지 않음")

    upload_parquet_bytes(buffer.read(), bucket, s3_key)
