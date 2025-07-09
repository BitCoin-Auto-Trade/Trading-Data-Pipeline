import os
import json
import redis
from dotenv import load_dotenv
from src.utils.logger import get_logger

load_dotenv()

class RedisUploader:
    """Redis 서버에 대한 연결 및 데이터 업로드를 처리합니다.
    """
    def __init__(self, host=None, port=None, db=None, password=None):
        """Redis 연결을 초기화합니다.
        """
        self.host = host or os.getenv("REDIS_HOST", "localhost")
        self.port = port or int(os.getenv("REDIS_PORT", 6379))
        self.db = db or int(os.getenv("REDIS_DB", 0))
        self.password = password or os.getenv("REDIS_PASSWORD", None)
        self.logger = get_logger(__name__)
        self.redis_client = None
        self._connect()

    def _connect(self):
        """Redis 서버에 대한 연결을 설정합니다.
        """
        try:
            self.redis_client = redis.Redis(
                host=self.host, 
                port=self.port, 
                db=self.db,
                password=self.password,  # 인증에 비밀번호를 사용합니다.
                decode_responses=True
            )
            self.redis_client.ping()
            self.logger.info(f"Redis에 성공적으로 연결되었습니다({self.host}:{self.port})")
        except redis.exceptions.ConnectionError as e:
            self.logger.error(f"Redis 연결에 실패했습니다: {e}")
            self.redis_client = None

    def is_connected(self):
        return self.redis_client is not None

    def upload_json(self, key: str, data: dict):
        if not self.is_connected():
            self.logger.error("데이터를 업로드할 수 없습니다. Redis에 연결되어 있지 않습니다.")
            return
        
        try:
            self.redis_client.set(key, json.dumps(data))
            self.logger.debug(f"JSON을 키에 성공적으로 업로드했습니다: {key}")
        except Exception as e:
            self.logger.error(f"키 {key}에 JSON을 업로드하지 못했습니다: {e}")

    def upload_dataframe(self, key: str, df):
        if not self.is_connected():
            self.logger.error("DataFrame을 업로드할 수 없습니다. Redis에 연결되어 있지 않습니다.")
            return
        
        try:
            data = df.to_json(orient='records')
            self.redis_client.set(key, data)
            self.logger.info(f"DataFrame을 키에 성공적으로 업로드했습니다: {key}")
        except Exception as e:
            self.logger.error(f"키 {key}에 DataFrame을 업로드하지 못했습니다: {e}")

if __name__ == '__main__':
    uploader = RedisUploader()

    if uploader.is_connected():
        sample_dict = {"price": 70000, "symbol": "BTCUSDT"}
        uploader.upload_json("binance:ticker:btcusdt", sample_dict)
        retrieved_data = uploader.redis_client.get("binance:ticker:btcusdt")
        print(f"Redis에서 검색됨: {retrieved_data}")