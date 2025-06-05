import time
import logging
from functools import wraps

logger = logging.getLogger(__name__)

def retry(max_attempts=5, delay=2):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    logger.warning(f"[retry {attempt}/{max_attempts}] {func.__name__} failed: {e}")
                    last_exception = e
                    time.sleep(delay)
            logger.error(f"{func.__name__} failed after {max_attempts} attempts")
            raise last_exception
        return wrapper
    return decorator
