import redis
import json
import os
from datetime import datetime

redis_client = redis.Redis(
    host=os.getenv("REDIS_HOST", "redis"),
    port=int(os.getenv("REDIS_PORT", 6379)),
    db=0,
)

def get_slot_number(ts: datetime) -> int:
    return (ts.minute % 15) + 1 

def upload_to_redis(df, symbol: str):
    if df.empty:
        return

    row = df.iloc[-1]
    ts = row["timestamp"]
    if isinstance(ts, str):
        ts = datetime.fromisoformat(ts)

    slot = get_slot_number(ts)
    key = f"ohlcv:{symbol}:{slot}"

    data = row.to_dict()
    data["timestamp"] = str(data["timestamp"])
    redis_client.set(key, json.dumps(data))
