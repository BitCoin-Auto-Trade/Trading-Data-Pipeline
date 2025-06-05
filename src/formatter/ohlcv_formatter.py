import logging
from pathlib import Path
import pandas as pd

logger = logging.getLogger(__name__)

def format_ohlcv(df: pd.DataFrame, symbol: str, interval: str) -> pd.DataFrame:
    df = df.sort_values("timestamp")
    df = df.drop_duplicates(subset=["timestamp", "symbol", "interval"])
    df["symbol"] = symbol
    df["interval"] = interval
    return df[["timestamp", "open", "high", "low", "close", "volume", "symbol", "interval"]]

def save_to_parquet(df: pd.DataFrame, output_path: Path) -> None:
    df.to_parquet(output_path, index=False)
    logger.info(f"Saved DataFrame to {output_path}")
