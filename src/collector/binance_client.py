import requests
import pandas as pd
from datetime import datetime
from typing import Literal

Interval = Literal["15m", "1h"]

def fetch_ohlcv(symbol: str, interval: Interval, start_time: datetime, end_time: datetime) -> pd.DataFrame:
    url = "https://fapi.binance.com/fapi/v1/klines"
    params = {
        "symbol": symbol,
        "interval": interval,
        "startTime": int(start_time.timestamp() * 1000),
        "endTime": int(end_time.timestamp() * 1000),
        "limit": 1000
    }

    response = requests.get(url, params=params)
    response.raise_for_status()

    data = response.json()
    df = pd.DataFrame(data, columns=[
        "open_time", "open", "high", "low", "close", "volume",
        "_1", "_2", "_3", "_4", "_5", "_6"
    ])
    df = df[["open_time", "open", "high", "low", "close", "volume"]]
    df["timestamp"] = pd.to_datetime(df["open_time"], unit="ms")
    df["symbol"] = symbol
    df["interval"] = interval

    return df[["timestamp", "open", "high", "low", "close", "volume", "symbol", "interval"]]
