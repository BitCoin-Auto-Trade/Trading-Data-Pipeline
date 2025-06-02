import boto3
from pathlib import Path
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def upload_to_s3(file_path: str, bucket: str, key: str) -> bool:
    s3 = boto3.client("s3")
    try:
        s3.upload_file(Filename=file_path, Bucket=bucket, Key=key)
        logger.info(f"Uploaded {file_path} to s3://{bucket}/{key}")
        return True
    except Exception as e:
        logger.error(f"Failed to upload {file_path} to S3: {e}")
        return False

def make_s3_key(symbol: str, interval: str, file_path: str) -> str:
    filename = Path(file_path).name
    return f"ohlcv/{interval}/{symbol}/{filename}"