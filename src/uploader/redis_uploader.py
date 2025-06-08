import redis
import json
import os
import pandas as pd
from datetime import datetime


redis_client = redis.Redis(
    host=os.getenv("REDIS_HOST", "redis"),
    port=int(os.getenv("REDIS_PORT", 6379)),
    db=0,
)

def get_slot_index(ts: pd.Timestamp | datetime) -> int:
    return (ts.minute % 15) + 1  # 1~15 슬롯 고정

def upload_to_redis(df, symbol: str):
    key = f"ohlcv:{symbol}"

    try:
        for row in df.to_dict(orient="records"):
            ts = pd.to_datetime(row["timestamp"])
            slot = get_slot_index(ts)
            row["timestamp"] = str(ts)

            redis_client.hset(key, slot, json.dumps(row))  # 슬롯 기반 덮어쓰기
    except Exception as e:
        print(f"[REDIS ERROR] {symbol}: {e}")