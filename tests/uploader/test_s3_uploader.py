from unittest.mock import patch
from src.uploader.s3_uploader import upload_parquet_bytes

@patch("src.uploader.s3_uploader.s3.put_object")
def test_upload_parquet_bytes(mock_put):
    mock_put.return_value = {}
    upload_parquet_bytes(b"data", "test-bucket", "folder/file.parquet")
    mock_put.assert_called_once()
