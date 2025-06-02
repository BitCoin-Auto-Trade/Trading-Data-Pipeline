import pytest
from unittest.mock import patch
from uploader.s3_uploader import upload_to_s3, make_s3_key

@patch("boto3.client")
def test_upload_to_s3_success(mock_boto_client):
    mock_s3 = mock_boto_client.return_value
    mock_s3.upload_file.return_value = None  # 성공 케이스

    result = upload_to_s3("dummy.parquet", "test-bucket", "path/to/dummy.parquet")

    mock_s3.upload_file.assert_called_once_with(
        Filename="dummy.parquet",
        Bucket="test-bucket",
        Key="path/to/dummy.parquet",
    )
    assert result is True

def test_make_s3_key():
    key = make_s3_key("BTCUSDT", "15m", "some/local/path/2024-05-01_12.parquet")
    assert key == "ohlcv/15m/BTCUSDT/2024-05-01_12.parquet"