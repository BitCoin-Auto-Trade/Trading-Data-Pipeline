import pandas as pd
from src.formatter.ohlcv_formatter import clean_ohlcv, save_to_parquet
from datetime import datetime

def test_clean_ohlcv_basic():
    data = {
        "timestamp": [datetime(2024, 1, 1, 0), datetime(2024, 1, 1, 0)],  # 중복
        "open": [1.0, 1.0],
        "high": [2.0, 2.0],
        "low": [0.5, 0.5],
        "close": [1.5, 1.5],
        "volume": [100, 100],
        "symbol": ["BTCUSDT", "BTCUSDT"],
        "interval": ["15m", "15m"]
    }
    df = pd.DataFrame(data)

    cleaned = clean_ohlcv(df)

    assert cleaned.shape[0] == 1  # 중복 제거 확인
    assert list(cleaned.columns) == [
        "timestamp", "open", "high", "low", "close", "volume", "symbol", "interval"
    ]
    assert cleaned["timestamp"].dt.tz is not None  # UTC 확인
