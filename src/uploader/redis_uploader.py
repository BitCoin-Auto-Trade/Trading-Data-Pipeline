import os
import json
import redis
from dotenv import load_dotenv
from src.utils.logger import get_logger

load_dotenv()

class RedisUploader:
    """Handles connections and data uploading to a Redis server.
    """
    def __init__(self, host=None, port=None, db=None, password=None):
        """Initializes the Redis connection.
        """
        self.host = host or os.getenv("REDIS_HOST", "localhost")
        self.port = port or int(os.getenv("REDIS_PORT", 6379))
        self.db = db or int(os.getenv("REDIS_DB", 0))
        self.password = password or os.getenv("REDIS_PASSWORD", None)
        self.logger = get_logger(__name__)
        self.redis_client = None
        self._connect()

    def _connect(self):
        """Establishes a connection to the Redis server.
        """
        try:
            self.redis_client = redis.Redis(
                host=self.host, 
                port=self.port, 
                db=self.db,
                password=self.password,  # Use the password for authentication
                decode_responses=True
            )
            self.redis_client.ping()
            self.logger.info(f"Successfully connected to Redis at {self.host}:{self.port}")
        except redis.exceptions.ConnectionError as e:
            self.logger.error(f"Failed to connect to Redis: {e}")
            self.redis_client = None

    def is_connected(self):
        return self.redis_client is not None

    def upload_json(self, key: str, data: dict):
        if not self.is_connected():
            self.logger.error("Cannot upload data. Redis is not connected.")
            return
        
        try:
            self.redis_client.set(key, json.dumps(data))
            self.logger.debug(f"Successfully uploaded JSON to key: {key}")
        except Exception as e:
            self.logger.error(f"Failed to upload JSON to key {key}: {e}")

    def upload_dataframe(self, key: str, df):
        if not self.is_connected():
            self.logger.error("Cannot upload DataFrame. Redis is not connected.")
            return
        
        try:
            data = df.to_json(orient='records')
            self.redis_client.set(key, data)
            self.logger.info(f"Successfully uploaded DataFrame to key: {key}")
        except Exception as e:
            self.logger.error(f"Failed to upload DataFrame to key {key}: {e}")

if __name__ == '__main__':
    uploader = RedisUploader()

    if uploader.is_connected():
        sample_dict = {"price": 70000, "symbol": "BTCUSDT"}
        uploader.upload_json("binance:ticker:btcusdt", sample_dict)
        retrieved_data = uploader.redis_client.get("binance:ticker:btcusdt")
        print(f"Retrieved from Redis: {retrieved_data}")