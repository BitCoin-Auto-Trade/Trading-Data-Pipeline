import boto3
import logging

def upload_to_s3_raw_bytes(raw_bytes: bytes, bucket: str, s3_key: str) -> bool:
    try:
        s3 = boto3.client("s3")
        s3.put_object(Bucket=bucket, Key=s3_key, Body=raw_bytes)
        logging.info(f"Uploaded to s3://{bucket}/{s3_key}")
        return True
    except Exception as e:
        logging.error(f"S3 upload failed: {e}")
        raise  # 예외 던져야 DAG가 실패로 뜸

