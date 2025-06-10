import pandas as pd

def clean_raw_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    if 'timestamp' not in df.columns:
        raise ValueError("Missing 'timestamp' column")

    df = df.copy()
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')

    cols = ['open', 'high', 'low', 'close', 'volume']
    for col in cols:
        df[col] = pd.to_numeric(df.get(col), errors='coerce')

    df = df.dropna(subset=['timestamp'] + cols)
    return df

def format_ohlcv(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    df = df.reset_index(drop=True)
    df = df.sort_values("timestamp")
    df = df.assign(symbol=symbol)
    df = df.drop_duplicates(subset=["timestamp", "symbol"])
    return df[["timestamp", "open", "high", "low", "close", "volume", "symbol"]]
