import pytest
from datetime import datetime, timedelta, UTC
from collector.binance_client import fetch_ohlcv

def test_fetch_ohlcv_returns_dataframe():
    symbol = "BTCUSDT"
    interval = "15m"
    end = datetime.now(UTC)
    start = end - timedelta(hours=1)

    df = fetch_ohlcv(symbol, interval, start, end)

    assert not df.empty
    assert "timestamp" in df.columns
    assert "symbol" in df.columns
