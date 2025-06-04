import logging
import boto3
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
