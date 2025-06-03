import pandas as pd
from formatter.ohlcv_formatter import format_ohlcv

def test_format_ohlcv_removes_duplicates_and_sorts():
    df = pd.DataFrame([
        {"timestamp": "2024-01-01 01:00:00", "open": 1, "high": 2, "low": 1, "close": 2, "volume": 10, "symbol": "BTCUSDT", "interval": "15m"},
        {"timestamp": "2024-01-01 01:00:00", "open": 1, "high": 2, "low": 1, "close": 2, "volume": 10, "symbol": "BTCUSDT", "interval": "15m"},
        {"timestamp": "2024-01-01 00:45:00", "open": 1, "high": 2, "low": 1, "close": 2, "volume": 10, "symbol": "BTCUSDT", "interval": "15m"},
    ])
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    formatted = format_ohlcv(df)

    assert len(formatted) == 2
    assert formatted.iloc[0]["timestamp"] < formatted.iloc[1]["timestamp"]
