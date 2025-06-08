from datetime import datetime, timedelta, UTC

SYMBOLS = ["BTCUSDT", "ETHUSDT"]  # 필요한 심볼 리스트

def now_range_1m(_: int = 0):
    now = datetime.now(tz=UTC)
    start = now.replace(second=0, microsecond=0)
    end = start + timedelta(minutes=1)
    return start, end

def get_time_range(minutes: int):
    now = datetime.now(tz=UTC)
    end = now.replace(second=0, microsecond=0)
    start = end - timedelta(minutes=minutes)
    return start, end
