from datetime import datetime, timedelta, UTC
from utils.logger import get_logger

SYMBOLS = ['BTCUSDT', 'ETHUSDT']

def get_time_range(delta_min: int) -> tuple[datetime, datetime]:
    now = datetime.now(UTC).replace(second=0, microsecond=0)
    end = now - timedelta(minutes=now.minute % delta_min)
    start = end - timedelta(minutes=delta_min)
    return start, end

def now_range_1m() -> tuple[datetime, datetime]:
    now = datetime.now(UTC).replace(second=0, microsecond=0)
    return now - timedelta(minutes=1), now
