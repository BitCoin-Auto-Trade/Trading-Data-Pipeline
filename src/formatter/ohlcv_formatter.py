import pandas as pd
from pathlib import Path

def clean_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    # 컬럼 통일
    df = df[["timestamp", "open", "high", "low", "close", "volume", "symbol", "interval"]]

    # 중복 제거 + 정렬
    df = df.drop_duplicates()
    df = df.sort_values("timestamp")

    # timestamp → UTC 보장 (to_parquet에도 영향)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

    return df

def save_to_parquet(df: pd.DataFrame, path: str) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
