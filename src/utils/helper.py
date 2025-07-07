def safe_get(d: dict, *keys, default=None):
    """중첩 dict 안전하게 조회"""
    for key in keys:
        if isinstance(d, dict) and key in d:
            d = d[key]
        else:
            return default
    return d