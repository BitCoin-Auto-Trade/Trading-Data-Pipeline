import json
import logging
import pandas as pd
from collector.redis_client import redis_client

logger = logging.getLogger(__name__)

def load_ohlcv_from_redis(symbol: str, interval: str = "1m") -> pd.DataFrame:
    key = f"ohlcv:{symbol}:{interval}"
    try:
        raw = redis_client.lrange(key, 0, -1)
        rows = [json.loads(item) for item in raw]
        df = pd.DataFrame(rows)
        if not df.empty:
            df["timestamp"] = pd.to_datetime(df["timestamp"])
        return df
    except Exception as e:
        logger.exception(f"[Redis] OHLCV 로딩 실패: {symbol}, {interval} - {e}")
        return pd.DataFrame()

def load_indicators_from_redis(symbol: str, interval: str = "1m") -> pd.DataFrame:
    key = f"indicator:{symbol}:{interval}"
    try:
        raw = redis_client.lrange(key, 0, -1)
        rows = [json.loads(item) for item in raw]
        df = pd.DataFrame(rows)
        if not df.empty:
            df["timestamp"] = pd.to_datetime(df["timestamp"])
        return df
    except Exception as e:
        logger.exception(f"[Redis] 지표 로딩 실패: {symbol}, {interval} - {e}")
        return pd.DataFrame()