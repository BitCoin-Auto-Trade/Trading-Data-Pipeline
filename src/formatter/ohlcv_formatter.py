import pandas as pd

def format_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values("timestamp").drop_duplicates()
    return df[["timestamp", "open", "high", "low", "close", "volume", "symbol", "interval"]]

def save_to_parquet(df: pd.DataFrame, output_path: str) -> None:
    df.to_parquet(output_path, index=False)
