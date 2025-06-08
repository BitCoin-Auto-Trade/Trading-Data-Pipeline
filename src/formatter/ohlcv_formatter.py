import pandas as pd

def normalize_interval(interval: str) -> str:
    mapping = {
        "1m": "1min",
        "3m": "3min",
        "5m": "5min",
        "15m": "15min",
        "30m": "30min",
        "1h": "1h",
        "2h": "2h",
        "4h": "4h",
        "1d": "1d",
    }
    return mapping.get(interval.lower(), interval.lower())

def clean_raw_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    df.set_index('timestamp', inplace=True)
    for col in ['open', 'high', 'low', 'close', 'volume']:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    return df.dropna(subset=['open', 'high', 'low', 'close', 'volume'])

def resample_ohlcv(df: pd.DataFrame, interval: str) -> pd.DataFrame:
    interval = normalize_interval(interval)
    resampled = df.resample(interval).agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    }).dropna()
    
    resampled.index = resampled.index.floor(interval)
    return resampled.reset_index()

def format_ohlcv(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    df = df.reset_index()
    df = df.sort_values("timestamp")
    df["symbol"] = symbol
    df = df.drop_duplicates(subset=["timestamp", "symbol"])
    return df[["timestamp", "open", "high", "low", "close", "volume", "symbol"]]