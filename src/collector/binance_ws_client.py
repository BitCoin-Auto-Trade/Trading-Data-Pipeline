import asyncio
import json
import time
import websockets
from src.utils.logger import get_logger
from src.uploader.redis_uploader import RedisUploader

class BinanceWebsocketClient:
    """Handles real-time data streams from Binance Futures WebSocket API.
    """
    BASE_URL = "wss://fstream.binance.com/stream?streams="

    def __init__(self, streams: list, on_message_callback):
        self.streams = streams
        self.on_message_callback = on_message_callback
        self.logger = get_logger(__name__)
        self.ws_url = f"{self.BASE_URL}{'/'.join(self.streams)}"
        self.is_running = False

    async def _connect_and_listen(self):
        self.logger.info(f"Connecting to WebSocket: {self.ws_url}")
        while self.is_running:
            try:
                async with websockets.connect(self.ws_url) as ws:
                    self.logger.info("WebSocket connection established.")
                    while self.is_running:
                        try:
                            message = await ws.recv()
                            data = json.loads(message)
                            # Pass the received data to the callback function
                            await self.on_message_callback(data)
                        except websockets.ConnectionClosed:
                            self.logger.warning("WebSocket connection closed. Reconnecting...")
                            break
                        except Exception as e:
                            self.logger.error(f"Error receiving message: {e}")
            except Exception as e:
                self.logger.error(f"WebSocket connection failed: {e}. Retrying in 5 seconds...")
                await asyncio.sleep(5)

    def start(self):
        if not self.is_running:
            self.is_running = True
            asyncio.run(self._connect_and_listen())

    def stop(self):
        self.logger.info("Stopping WebSocket client.")
        self.is_running = False

# --- Optimized Integration Logic ---

redis_uploader = RedisUploader()
# Dictionary to track the last update time for throttling
last_update_times = {}
# Max length for the trade list in Redis
TRADE_LIST_MAX_LEN = 100

async def handle_and_upload_message(data):
    """Callback function to process data and upload it to Redis with optimization.
    """
    if not redis_uploader.is_connected():
        # Using print for visibility in this example
        print("Redis is not connected. Cannot upload data.")
        return

    stream = data.get('stream')
    payload = data.get('data')
    symbol = payload.get('s', '').lower()
    now = time.time()

    if stream.endswith('@kline_1m'):
        kline = payload.get('k')
        if kline.get('x'):  # Process only closed klines
            redis_key = f"binance:kline:{symbol}:1m"
            redis_uploader.upload_json(redis_key, kline)
            print(f"Uploaded 1m kline for {symbol} to Redis.")

    elif stream.endswith('@depth5@100ms'):
        # Throttle depth updates to once per second
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
        # Use a capped list for recent trades
        redis_key = f"binance:trades:{symbol}"
        trade_data = json.dumps(payload)
        # Atomically push to list and trim it
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
        print("Starting WebSocket client to upload optimized data to Redis...")
        ws_client.start()
    except KeyboardInterrupt:
        ws_client.stop()
        print("WebSocket client stopped by user.")
