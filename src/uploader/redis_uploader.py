import os
import json
import redis
import pandas as pd
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

redis_client = redis.Redis(
    host=os.getenv("REDIS_HOST", "redis"),
    port=int(os.getenv("REDIS_PORT", 6379)),
    db=0,
    decode_responses=True,
)

MAX_CANDLE_COUNT = 15

def upload_to_redis(df: pd.DataFrame, symbol: str) -> None:
    """Redis에 OHLCV 1분봉 최근 15개 유지."""
    if df.empty:
        logger.info(f"[Redis] {symbol}: 빈 데이터 생략")
        return

    key = f"ohlcv:{symbol}:1m"

    try:
        existing_raw = redis_client.lrange(key, 0, -1)
        existing_rows = [json.loads(item) for item in existing_raw]

        new_rows = df.sort_values("timestamp").to_dict(orient="records")

        combined_df = pd.DataFrame(existing_rows + new_rows)
        combined_df["timestamp"] = pd.to_datetime(combined_df["timestamp"])
        combined_df = combined_df.sort_values("timestamp")
        deduped_df = combined_df.drop_duplicates(subset="timestamp", keep="last")

        deduped_df = deduped_df.sort_values("timestamp")[-MAX_CANDLE_COUNT:]

        pipe = redis_client.pipeline()
        pipe.delete(key)
        for row in deduped_df.to_dict(orient="records"):
            row["timestamp"] = pd.to_datetime(row["timestamp"]).isoformat()
            pipe.rpush(key, json.dumps(row))
        pipe.expire(key, 60 * 5)
        pipe.execute()

        logger.info(f"[Redis] {symbol}: {len(deduped_df)}개 저장 완료")
    except Exception as e:
        logger.exception(f"[Redis] {symbol} 저장 실패: {e}")
