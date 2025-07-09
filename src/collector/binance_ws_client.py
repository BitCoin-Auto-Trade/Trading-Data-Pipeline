import asyncio
import json
import time
import websockets
from src.utils.logger import get_logger
from src.uploader.redis_uploader import RedisUploader

class BinanceWebsocketClient:
    """바이낸스 선물 WebSocket API의 실시간 데이터 스트림을 처리합니다.
    """
    BASE_URL = "wss://fstream.binance.com/stream?streams="

    def __init__(self, streams: list, on_message_callback):
        self.streams = streams
        self.on_message_callback = on_message_callback
        self.logger = get_logger(__name__)
        self.ws_url = f"{self.BASE_URL}{'/'.join(self.streams)}"
        self.is_running = False

    async def _connect_and_listen(self):
        self.logger.info(f"WebSocket에 연결 중: {self.ws_url}")
        while self.is_running:
            try:
                async with websockets.connect(self.ws_url) as ws:
                    self.logger.info("WebSocket 연결이 설정되었습니다.")
                    while self.is_running:
                        try:
                            message = await ws.recv()
                            data = json.loads(message)
                            # 수신된 데이터를 콜백 함수에 전달합니다.
                            await self.on_message_callback(data)
                        except websockets.ConnectionClosed:
                            self.logger.warning("WebSocket 연결이 닫혔습니다. 다시 연결 중...")
                            break
                        except Exception as e:
                            self.logger.error(f"메시지 수신 중 오류 발생: {e}")
            except Exception as e:
                self.logger.error(f"WebSocket 연결에 실패했습니다: {e}. 5초 후에 다시 시도합니다...")
                await asyncio.sleep(5)

    def start(self):
        if not self.is_running:
            self.is_running = True
            asyncio.run(self._connect_and_listen())

    def stop(self):
        self.logger.info("WebSocket 클라이언트를 중지합니다.")
        self.is_running = False

# --- 최적화된 통합 로직 ---

redis_uploader = RedisUploader()
# 조절을 위해 마지막 업데이트 시간을 추적하는 사전
last_update_times = {}
# Redis의 거래 목록 최대 길이
TRADE_LIST_MAX_LEN = 100

async def handle_and_upload_message(data):
    """최적화를 통해 데이터를 처리하고 Redis에 업로드하는 콜백 함수입니다.
    """
    if not redis_uploader.is_connected():
        # 이 예제에서는 가시성을 위해 print를 사용합니다.
        print("Redis에 연결되지 않았습니다. 데이터를 업로드할 수 없습니다.")
        return

    stream = data.get('stream')
    payload = data.get('data')
    symbol = payload.get('s', '').lower()
    now = time.time()

    if stream.endswith('@kline_1m'):
        kline = payload.get('k')
        if kline.get('x'):  # 닫힌 kline만 처리
            redis_key = f"binance:kline:{symbol}:1m"
            redis_uploader.upload_json(redis_key, kline)
            print(f"{symbol}에 대한 1분 kline을 Redis에 업로드했습니다.")

    elif stream.endswith('@depth5@100ms'):
        # 깊이 업데이트를 초당 한 번으로 조절
        if now - last_update_times.get(stream, 0) > 1:
            redis_key = f"binance:depth:{symbol}"
            simplified_depth = {
                "bids": payload.get('b', [])[:5],
                "asks": payload.get('a', [])[:5],
                "ts": payload.get('E')
            }
            redis_uploader.upload_json(redis_key, simplified_depth)
            last_update_times[stream] = now

    elif stream.endswith('@aggTrade'):
        # 최근 거래에 대해 제한된 목록 사용
        redis_key = f"binance:trades:{symbol}"
        trade_data = json.dumps(payload)
        # 원자적으로 목록에 푸시하고 다듬습니다.
        pipe = redis_uploader.redis_client.pipeline()
        pipe.lpush(redis_key, trade_data)
        pipe.ltrim(redis_key, 0, TRADE_LIST_MAX_LEN - 1)
        pipe.execute()

if __name__ == '__main__':
    target_streams = [
        "btcusdt@kline_1m",
        "btcusdt@depth5@100ms",
        "btcusdt@aggTrade"
    ]

    ws_client = BinanceWebsocketClient(
        streams=target_streams,
        on_message_callback=handle_and_upload_message
    )

    try:
        print("최적화된 데이터를 Redis에 업로드하기 위해 WebSocket 클라이언트를 시작합니다...")
        ws_client.start()
    except KeyboardInterrupt:
        ws_client.stop()
        print("사용자에 의해 WebSocket 클라이언트가 중지되었습니다.")
