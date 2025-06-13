import logging
from datetime import datetime
from typing import Literal
from collector.retry import retry
import requests
import pandas as pd

logger = logging.getLogger(__name__)

Interval = Literal["15m", "1h"]

@retry(max_attempts=5, delay=2)
def fetch_ohlcv(symbol: str, interval: Interval, start_time: datetime, end_time: datetime) -> pd.DataFrame:
    """Binance API에서 OHLCV 데이터를 가져옵니다."""
    url = "https://fapi.binance.com/fapi/v1/klines"
    params = {
        "symbol": symbol,
        "interval": interval,
        "startTime": int(start_time.timestamp() * 1000),
        "endTime": int(end_time.timestamp() * 1000),
        "limit": 1000
    }

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logger.error(f"Binance API request failed: {e}")
        raise

    data = response.json()
    df = pd.DataFrame(data)[[0, 1, 2, 3, 4, 5]]
    df.columns = ["open_time", "open", "high", "low", "close", "volume"]

    df["timestamp"] = pd.to_datetime(df["open_time"], unit="ms")
    df["symbol"] = symbol
    df["interval"] = interval

    return df[["timestamp", "open", "high", "low", "close", "volume", "symbol", "interval"]]
