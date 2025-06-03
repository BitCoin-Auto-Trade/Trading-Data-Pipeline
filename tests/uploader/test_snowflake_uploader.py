from unittest.mock import patch, MagicMock
from uploader.snowflake_uploader import load_parquet_to_snowflake

@patch("uploader.snowflake_uploader.snowflake.connector.connect")
def test_load_parquet_to_snowflake_success(mock_connect):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_connect.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cursor

    result = load_parquet_to_snowflake(
        file_path="2024-06-01_12.parquet",
        table_name="OHLCV_TABLE",
        stage_name="MY_STAGE",
        file_format="MY_FORMAT"
    )

    mock_connect.assert_called_once()
    mock_cursor.execute.assert_called_once()
    assert result is True