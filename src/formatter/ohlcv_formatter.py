import logging
import pandas as pd

logger = logging.getLogger(__name__)

def format_ohlcv(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    df = df.sort_values("timestamp")
    df["symbol"] = symbol
    df = df.drop_duplicates(subset=["timestamp", "symbol"])
    return df[["timestamp", "open", "high", "low", "close", "volume", "symbol"]]
