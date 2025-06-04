import pandas as pd
from datetime import datetime, UTC
from pathlib import Path
from tempfile import TemporaryDirectory
from src.formatter.ohlcv_formatter import format_ohlcv, save_to_parquet

def test_format_ohlcv():
    now = datetime.now(UTC)
    df = pd.DataFrame([{
        "timestamp": now,
        "open": "100", "high": "110", "low": "90", "close": "105", "volume": "123.45",
        "symbol": "BTCUSDT", "interval": "15m"
    }] * 2)
    formatted = format_ohlcv(df)
    assert len(formatted) == 1

def test_save_to_parquet():
    df = pd.DataFrame([{
        "timestamp": datetime.now(UTC),
        "open": "100", "high": "110", "low": "90", "close": "105", "volume": "123.45",
        "symbol": "BTCUSDT", "interval": "15m"
    }])
    with TemporaryDirectory() as tmp:
        path = Path(tmp) / "ohlcv.parquet"
        save_to_parquet(df, path)
        loaded = pd.read_parquet(path)
        assert loaded.equals(df)
