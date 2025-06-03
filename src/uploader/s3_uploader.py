import boto3
import logging
import os

def make_s3_key(symbol: str, interval: str, file_path: str) -> str:
    basename = os.path.basename(file_path)
    return f"ohlcv/{interval}/{symbol}/{basename}"

def upload_to_s3(file_path: str, bucket: str, s3_key: str) -> bool:
    try:
        s3 = boto3.client("s3")
        s3.upload_file(Filename=file_path, Bucket=bucket, Key=s3_key)
        logging.info(f"Uploaded to s3://{bucket}/{s3_key}")
        return True
    except Exception as e:
        logging.error(f"S3 upload failed: {e}")
        return False
