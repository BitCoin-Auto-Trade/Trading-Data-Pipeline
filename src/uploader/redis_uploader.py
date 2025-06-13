import json
import logging
import pandas as pd
from collector.redis_client import redis_client

logger = logging.getLogger(__name__)

MAX_COUNTS = {
    "1m": 60,
    "15m": 300,
    "1h": 300,
}
TTL_SECONDS = 60 * 10  # 10분 TTL

def upload_to_redis(df: pd.DataFrame, symbol: str) -> None:
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
        combined_df = combined_df.sort_values("timestamp").drop_duplicates(subset="timestamp", keep="last")
        deduped_df = combined_df[-MAX_COUNTS["1m"]:]

        pipe = redis_client.pipeline()
        pipe.delete(key)
        for row in deduped_df.to_dict(orient="records"):
            row["timestamp"] = pd.to_datetime(row["timestamp"]).isoformat()
            pipe.rpush(key, json.dumps(row))
        pipe.expire(key, TTL_SECONDS)
        pipe.execute()

        logger.info(f"[Redis] {symbol}: {len(deduped_df)}개 저장 완료")
    except Exception as e:
        logger.exception(f"[Redis] {symbol} 저장 실패: {e}")

def upload_indicators_to_redis(df: pd.DataFrame, symbol: str, interval: str = "1m") -> None:
    if df.empty:
        logger.info(f"[Redis] {symbol}: 지표 데이터 없음 ({interval})")
        return

    key = f"indicator:{symbol}:{interval}"
    max_count = MAX_COUNTS.get(interval, 60)

    try:
        new_rows = df.sort_values("timestamp").to_dict(orient="records")

        pipe = redis_client.pipeline()
        pipe.delete(key)
        for row in new_rows[-max_count:]:
            row["timestamp"] = pd.to_datetime(row["timestamp"]).isoformat()
            pipe.rpush(key, json.dumps(row))
        pipe.expire(key, TTL_SECONDS)
        pipe.execute()

        logger.info(f"[Redis] {symbol} 지표 {len(new_rows)}개 저장 완료 ({interval})")
    except Exception as e:
        logger.exception(f"[Redis] {symbol} 지표 저장 실패 ({interval}): {e}")