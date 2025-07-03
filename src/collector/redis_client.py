def get_redis_client():
    import redis
    import os

    return redis.Redis(
        host=os.getenv("REDIS_HOST"),
        port=int(os.getenv("REDIS_PORT")),
        password=os.getenv("REDIS_PASSWORD"),
        db=0,
        decode_responses=True,
    )
