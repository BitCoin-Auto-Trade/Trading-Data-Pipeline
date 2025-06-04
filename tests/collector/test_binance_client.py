import pandas as pd
from datetime import datetime
from src.collector.binance_client import fetch_ohlcv

def test_fetch_ohlcv(monkeypatch):
    now = datetime.utcnow()
    dummy_response = [[
        1625097600000, "34000.0", "34100.0", "33900.0", "34050.0", "123.456",
        0, 0, 0, 0, 0, 0
    ]]

    class MockResponse:
        def raise_for_status(self): pass
        def json(self): return dummy_response

    def mock_get(*args, **kwargs):
        return MockResponse()

    monkeypatch.setattr("requests.get", mock_get)
    df = fetch_ohlcv("BTCUSDT", "15m", now, now)
    assert not df.empty
    assert set(df.columns) == {
        "timestamp", "open", "high", "low", "close", "volume", "symbol", "interval"
    }
