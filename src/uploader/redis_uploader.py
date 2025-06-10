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
    """
    Redis에 OHLCV 1분봉 최근 15개 유지
    """
    if df.empty:
        logger.info(f"[Redis] {symbol}: 빈 데이터 생략")
        return

    key = f"ohlcv:{symbol}:1m"
    df = df.sort_values("timestamp")
    rows = df.to_dict(orient="records")

    try:
        pipe = redis_client.pipeline()

        for row in rows:
            ts = pd.to_datetime(row["timestamp"])
            row["timestamp"] = ts.isoformat()
            value = json.dumps(row)
            pipe.rpush(key, value)

        pipe.ltrim(key, -MAX_CANDLE_COUNT, -1)
        pipe.expire(key, 60 * 5)
        pipe.execute()

        logger.info(f"[Redis] {symbol}: {len(rows)}개 저장 완료")
    except Exception as e:
        logger.exception(f"[Redis] {symbol} 저장 실패: {e}")
