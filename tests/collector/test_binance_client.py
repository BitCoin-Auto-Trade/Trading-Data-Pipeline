import pytest
from datetime import datetime, timedelta
from src.collector.binance_client import fetch_ohlcv

def test_fetch_ohlcv_returns_dataframe():
    symbol = "BTCUSDT"
    interval = "15m"
    end = datetime.utcnow()
    start = end - timedelta(minutes=15)

    df = fetch_ohlcv(symbol, interval, start, end)

    assert not df.empty, "빈 데이터프레임이 반환됨"
    assert set(df.columns) == {
        "timestamp", "open", "high", "low", "close", "volume", "symbol", "interval"
    }, "컬럼 구조 불일치"
    assert df["symbol"].unique()[0] == symbol
    assert df["interval"].unique()[0] == interval
