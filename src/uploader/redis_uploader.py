import logging

logger = logging.getLogger(__name__)

MAX_COUNTS = {
    "1m": 60,
    "15m": 300,
    "1h": 300,
}
TTL_SECONDS = 60 * 10  # 10분 TTL

def upload_to_redis(df, symbol: str) -> None:
    """Redis에 OHLCV 데이터를 업로드"""
    if df is None or df.empty:
        logger.info(f"[Redis] {symbol}: 빈 데이터 생략")
        return

    import json
    import pandas as pd
    from collector.redis_client import get_redis_client

    redis_client = get_redis_client()
    key = f"ohlcv:{symbol}:1m"

    try:
        existing_raw = redis_client.lrange(key, 0, -1)
        existing_rows = [json.loads(item) for item in existing_raw]
        new_rows = df.sort_values("timestamp").to_dict(orient="records")

        combined_df = pd.DataFrame(existing_rows + new_rows)
        combined_df["timestamp"] = pd.to_datetime(combined_df["timestamp"])
        combined_df = (
            combined_df.sort_values("timestamp")
            .drop_duplicates(subset="timestamp", keep="last")
        )
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
