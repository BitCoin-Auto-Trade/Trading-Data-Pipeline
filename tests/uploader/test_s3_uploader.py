from unittest.mock import patch, MagicMock
from uploader.snowflake_uploader import load_parquet_to_snowflake

@patch("uploader.snowflake_uploader.snowflake")
def test_load_parquet_to_snowflake_success(mock_snowflake):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_snowflake.connector.connect.return_value = mock_conn

    result = load_parquet_to_snowflake(
        file_path="/tmp/2024-01-01_12.parquet",
        table_name="OHLCV_TABLE",
        stage_name="MY_STAGE",
        file_format="MY_FORMAT"
    )

    assert result is True
    mock_cursor.execute.assert_called_once()
    mock_conn.close.assert_called_once()

@patch("uploader.snowflake_uploader.snowflake")
def test_load_parquet_to_snowflake_failure(mock_snowflake):
    mock_snowflake.connector.connect.side_effect = Exception("Connection Failed")

    result = load_parquet_to_snowflake(
        file_path="/tmp/2024-01-01_12.parquet",
        table_name="OHLCV_TABLE",
        stage_name="MY_STAGE",
        file_format="MY_FORMAT"
    )

    assert result is False
