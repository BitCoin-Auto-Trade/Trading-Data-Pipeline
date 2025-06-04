from unittest.mock import patch, MagicMock
from src.uploader.snowflake_uploader import load_parquet_to_snowflake

@patch("snowflake.connector.connect")
def test_load_parquet_to_snowflake(mock_connect):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_connect.return_value = mock_conn

    load_parquet_to_snowflake(
        s3_key="ohlcv/15m/BTCUSDT/2024-01-01_00.parquet",
        table_name="ohlcv_data",
        stage_name="my_stage",
        file_format="parquet_format"
    )

    mock_cursor.execute.assert_called_once()
    mock_conn.close.assert_called_once()
