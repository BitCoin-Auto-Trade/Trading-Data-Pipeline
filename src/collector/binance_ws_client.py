"""
강화된 바이낸스 WebSocket 클라이언트
- 자동 재연결, 데이터 검증, 성능 모니터링, 안정성 강화
"""
import asyncio
import json
import time
import websockets
import threading
from typing import Dict, List, Optional, Callable, Any
from dataclasses import dataclass
from datetime import datetime, timedelta
from src.utils.logger import get_logger
from src.uploader.redis_uploader import RedisUploader

@dataclass
class ConnectionStats:
    """연결 통계"""
    connect_time: Optional[datetime] = None
    disconnect_time: Optional[datetime] = None
    total_messages: int = 0
    error_count: int = 0
    reconnect_count: int = 0
    last_message_time: Optional[datetime] = None
    
    @property
    def uptime_seconds(self) -> float:
        if self.connect_time and not self.disconnect_time:
            return (datetime.now() - self.connect_time).total_seconds()
        return 0.0
    
    @property
    def is_healthy(self) -> bool:
        if not self.last_message_time:
            return False
        time_since_last = (datetime.now() - self.last_message_time).total_seconds()
        return time_since_last < 60  # 60초 이내 메시지 수신 시 건강함

class RobustBinanceWebSocketClient:
    """바이낸스 WebSocket 클라이언트"""
    
    BASE_URL = "wss://fstream.binance.com/stream?streams="
    
    def __init__(self, streams: List[str], on_message_callback: Callable):
        self.streams = streams
        self.on_message_callback = on_message_callback
        self.logger = get_logger(__name__)
        
        # 연결 설정
        self.ws_url = f"{self.BASE_URL}{'/'.join(self.streams)}"
        self.is_running = False
        self.websocket = None
        
        # 재연결 설정
        self.max_reconnect_attempts = 10
        self.reconnect_delay = 5.0  # 초기 재연결 지연
        self.max_reconnect_delay = 300.0  # 최대 5분
        self.reconnect_backoff = 1.5  # 지수 백오프
        
        # 성능 모니터링
        self.stats = ConnectionStats()
        self.message_rates: Dict[str, int] = {}  # 스트림별 메시지 수신율
        self.last_rate_check = time.time()
        
        # 데이터 검증 설정
        self.enable_data_validation = True
        self.invalid_message_count = 0
        self.max_invalid_messages = 100  # 최대 허용 무효 메시지 수
        
        # 헬스 체크
        self.health_check_interval = 30  # 30초마다 헬스 체크
        self.last_health_check = time.time()
        
        # 스레드 안전성
        self._lock = threading.Lock()
        
        self.logger.info(f"WebSocket 클라이언트 초기화: {len(streams)}개 스트림")
    
    def _validate_message(self, data: Dict[str, Any]) -> bool:
        """메시지 유효성 검증"""
        if not self.enable_data_validation:
            return True
        
        try:
            # 기본 구조 검증
            if not isinstance(data, dict):
                return False
            
            if 'stream' not in data or 'data' not in data:
                return False
            
            stream = data.get('stream', '')
            payload = data.get('data', {})
            
            # 스트림별 검증
            if stream.endswith('@kline_1m'):
                return self._validate_kline_data(payload)
            elif stream.endswith('@depth5@100ms'):
                return self._validate_depth_data(payload)
            elif stream.endswith('@aggTrade'):
                return self._validate_trade_data(payload)
            
            return True
            
        except Exception as e:
            self.logger.warning(f"메시지 검증 중 오류: {e}")
            return False
    
    def _validate_kline_data(self, kline_data: Dict) -> bool:
        """K-line 데이터 검증"""
        if not isinstance(kline_data, dict):
            return False
        
        kline = kline_data.get('k', {})
        if not isinstance(kline, dict):
            return False
        
        required_fields = ['t', 'T', 's', 'o', 'c', 'h', 'l', 'v']
        return all(field in kline for field in required_fields)
    
    def _validate_depth_data(self, depth_data: Dict) -> bool:
        """Depth 데이터 검증"""
        if not isinstance(depth_data, dict):
            return False
        
        bids = depth_data.get('b', [])
        asks = depth_data.get('a', [])
        
        return (isinstance(bids, list) and isinstance(asks, list) and
                len(bids) > 0 and len(asks) > 0)
    
    def _validate_trade_data(self, trade_data: Dict) -> bool:
        """거래 데이터 검증"""
        if not isinstance(trade_data, dict):
            return False
        
        required_fields = ['s', 'p', 'q', 'm']
        return all(field in trade_data for field in required_fields)
    
    def _update_message_rates(self, stream: str):
        """메시지 수신율 업데이트"""
        current_time = time.time()
        
        with self._lock:
            if stream not in self.message_rates:
                self.message_rates[stream] = 0
            
            self.message_rates[stream] += 1
            
            # 1분마다 통계 리셋
            if current_time - self.last_rate_check > 60:
                self.logger.debug(f"메시지 수신율: {self.message_rates}")
                self.message_rates.clear()
                self.last_rate_check = current_time
    
    def _perform_health_check(self):
        """연결 상태 헬스 체크"""
        current_time = time.time()
        
        if current_time - self.last_health_check < self.health_check_interval:
            return
        
        self.last_health_check = current_time
        
        # 통계 업데이트
        with self._lock:
            is_healthy = self.stats.is_healthy
            uptime = self.stats.uptime_seconds
            
            if not is_healthy:
                self.logger.warning(
                    f"WebSocket 연결 상태 불량: "
                    f"마지막 메시지로부터 {(datetime.now() - self.stats.last_message_time).total_seconds():.1f}초 경과"
                )
            else:
                self.logger.debug(f"WebSocket 연결 상태 양호: 가동시간 {uptime:.1f}초")
            
            # 무효 메시지 너무 많으면 경고
            if self.invalid_message_count > self.max_invalid_messages:
                self.logger.error(f"무효 메시지가 {self.invalid_message_count}개 감지됨. 연결 품질 확인 필요")
                self.invalid_message_count = 0  # 리셋
    
    async def _handle_message(self, message: str):
        """메시지 처리 (검증 및 콜백 호출)"""
        try:
            data = json.loads(message)
            
            # 메시지 검증
            if not self._validate_message(data):
                self.invalid_message_count += 1
                self.logger.warning(f"유효하지 않은 메시지: {message[:100]}...")
                return
            
            # 통계 업데이트
            with self._lock:
                self.stats.total_messages += 1
                self.stats.last_message_time = datetime.now()
            
            # 스트림별 수신율 업데이트
            stream = data.get('stream', 'unknown')
            self._update_message_rates(stream)
            
            # 콜백 호출
            await self.on_message_callback(data)
            
        except json.JSONDecodeError as e:
            self.logger.error(f"JSON 파싱 오류: {e}")
            self.invalid_message_count += 1
        except Exception as e:
            self.logger.error(f"메시지 처리 중 오류: {e}")
            with self._lock:
                self.stats.error_count += 1
    
    async def _connect_and_listen(self):
        """연결 및 메시지 수신 (재연결 로직 포함)"""
        reconnect_attempts = 0
        reconnect_delay = self.reconnect_delay
        
        while self.is_running:
            try:
                self.logger.info(f"WebSocket 연결 시도: {self.ws_url}")
                
                # 연결 타임아웃 설정
                async with websockets.connect(
                    self.ws_url,
                    ping_interval=20,  # 20초마다 ping
                    ping_timeout=10,   # ping 타임아웃 10초
                    close_timeout=10,  # close 타임아웃 10초
                    max_size=1024*1024,  # 최대 메시지 크기 1MB
                ) as websocket:
                    self.websocket = websocket
                    
                    # 연결 성공 시 통계 업데이트
                    with self._lock:
                        self.stats.connect_time = datetime.now()
                        self.stats.disconnect_time = None
                        if reconnect_attempts > 0:
                            self.stats.reconnect_count += 1
                    
                    reconnect_attempts = 0  # 재연결 카운터 리셋
                    reconnect_delay = self.reconnect_delay  # 지연 시간 리셋
                    
                    self.logger.info("WebSocket 연결이 설정되었습니다.")
                    
                    # 메시지 수신 루프
                    while self.is_running:
                        try:
                            message = await websocket.recv()
                            await self._handle_message(message)
                            
                            # 주기적 헬스 체크
                            self._perform_health_check()
                            
                        except websockets.ConnectionClosed:
                            self.logger.warning("WebSocket 연결이 닫혔습니다.")
                            break
                        except asyncio.TimeoutError:
                            self.logger.warning("WebSocket 메시지 수신 타임아웃")
                            break
                        except Exception as e:
                            self.logger.error(f"메시지 수신 중 오류: {e}")
                            with self._lock:
                                self.stats.error_count += 1
                            break
                
            except websockets.InvalidURI:
                self.logger.error(f"유효하지 않은 WebSocket URI: {self.ws_url}")
                break
            except websockets.InvalidStatusCode as e:
                self.logger.error(f"WebSocket 연결 상태 코드 오류: {e}")
                reconnect_attempts += 1
            except Exception as e:
                self.logger.error(f"WebSocket 연결 오류: {e}")
                reconnect_attempts += 1
            
            # 재연결 시도
            if self.is_running and reconnect_attempts < self.max_reconnect_attempts:
                with self._lock:
                    self.stats.disconnect_time = datetime.now()
                
                self.logger.warning(
                    f"재연결 시도 {reconnect_attempts}/{self.max_reconnect_attempts} "
                    f"({reconnect_delay:.1f}초 후)"
                )
                
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * self.reconnect_backoff, self.max_reconnect_delay)
            else:
                if reconnect_attempts >= self.max_reconnect_attempts:
                    self.logger.error("최대 재연결 시도 횟수에 도달했습니다. 연결을 중단합니다.")
                break
        
        # 연결 종료 시 통계 업데이트
        with self._lock:
            self.stats.disconnect_time = datetime.now()
        
        self.websocket = None
        self.logger.info("WebSocket 연결이 종료되었습니다.")
    
    def start(self):
        """WebSocket 클라이언트 시작"""
        if self.is_running:
            self.logger.warning("WebSocket 클라이언트가 이미 실행 중입니다.")
            return
        
        self.logger.info("WebSocket 클라이언트를 시작합니다.")
        self.is_running = True
        
        # 비동기 이벤트 루프에서 실행
        asyncio.run(self._connect_and_listen())
    
    def stop(self):
        """WebSocket 클라이언트 중지"""
        self.logger.info("WebSocket 클라이언트를 중지합니다.")
        self.is_running = False
        
        if self.websocket:
            asyncio.create_task(self.websocket.close())
    
    def get_connection_stats(self) -> Dict[str, Any]:
        """연결 통계 조회"""
        with self._lock:
            return {
                "is_running": self.is_running,
                "is_connected": self.websocket is not None,
                "is_healthy": self.stats.is_healthy,
                "uptime_seconds": self.stats.uptime_seconds,
                "total_messages": self.stats.total_messages,
                "error_count": self.stats.error_count,
                "reconnect_count": self.stats.reconnect_count,
                "invalid_message_count": self.invalid_message_count,
                "connect_time": self.stats.connect_time.isoformat() if self.stats.connect_time else None,
                "last_message_time": self.stats.last_message_time.isoformat() if self.stats.last_message_time else None,
                "message_rates": self.message_rates.copy(),
                "streams": self.streams
            }
    
    def reset_stats(self):
        """통계 초기화"""
        with self._lock:
            self.stats = ConnectionStats()
            self.message_rates.clear()
            self.invalid_message_count = 0
        self.logger.info("WebSocket 통계가 초기화되었습니다.")

class DataHandler:
    """최적화된 데이터 처리기"""
    
    def __init__(self):
        self.redis_uploader = RedisUploader()
        self.logger = get_logger(__name__)
        
        # 성능 최적화 설정
        self.last_update_times: Dict[str, float] = {}
        self.trade_list_max_len = 100
        self.depth_update_interval = 1.0  # 1초마다 업데이트
        
        # 배치 처리 설정
        self.batch_size = 10
        self.trade_batch: List[str] = []
        self.last_batch_time = time.time()
        self.batch_interval = 2.0  # 2초마다 배치 처리
        
        # 성능 모니터링
        self.processed_count = 0
        self.error_count = 0
        self.start_time = time.time()
    
    async def handle_and_upload_message(self, data: Dict[str, Any]):
        """최적화된 메시지 처리 및 업로드"""
        if not self.redis_uploader.is_connected():
            self.logger.error("Redis에 연결되지 않았습니다. 데이터를 업로드할 수 없습니다.")
            return
        
        try:
            stream = data.get('stream')
            payload = data.get('data')
            symbol = payload.get('s', '').lower()
            current_time = time.time()
            
            if stream.endswith('@kline_1m'):
                await self._handle_kline_data(payload, symbol)
            elif stream.endswith('@depth5@100ms'):
                await self._handle_depth_data(payload, symbol, current_time)
            elif stream.endswith('@aggTrade'):
                await self._handle_trade_data(payload, symbol, current_time)
            
            self.processed_count += 1
            
        except Exception as e:
            self.logger.error(f"데이터 처리 중 오류: {e}")
            self.error_count += 1
    
    async def _handle_kline_data(self, payload: Dict, symbol: str):
        """K-line 데이터 처리"""
        kline = payload.get('k')
        if kline and kline.get('x'):  # 닫힌 kline만 처리
            redis_key = f"binance:kline:{symbol}:1m"
            
            # 데이터 정제
            clean_kline = {
                't': kline['t'],
                'T': kline['T'],
                's': kline['s'],
                'o': kline['o'],
                'c': kline['c'],
                'h': kline['h'],
                'l': kline['l'],
                'v': kline['v'],
                'x': kline['x']
            }
            
            self.redis_uploader.upload_json(redis_key, clean_kline)
            self.logger.debug(f"{symbol}에 대한 1분 kline을 Redis에 업로드했습니다.")
    
    async def _handle_depth_data(self, payload: Dict, symbol: str, current_time: float):
        """Depth 데이터 처리 (throttling 적용)"""
        stream_key = f"depth:{symbol}"
        
        # throttling: 1초에 한 번만 업데이트
        if current_time - self.last_update_times.get(stream_key, 0) < self.depth_update_interval:
            return
        
        self.last_update_times[stream_key] = current_time
        
        redis_key = f"binance:depth:{symbol}"
        simplified_depth = {
            "bids": payload.get('b', [])[:5],
            "asks": payload.get('a', [])[:5],
            "ts": payload.get('E'),
            "lastUpdateId": payload.get('u')
        }
        
        self.redis_uploader.upload_json(redis_key, simplified_depth)
        self.logger.debug(f"{symbol}에 대한 depth 데이터를 Redis에 업로드했습니다.")
    
    async def _handle_trade_data(self, payload: Dict, symbol: str, current_time: float):
        """거래 데이터 처리 (배치 처리)"""
        redis_key = f"binance:trades:{symbol}"
        
        # 데이터 정제
        clean_trade = {
            's': payload['s'],
            'p': payload['p'],
            'q': payload['q'],
            'm': payload['m'],
            'T': payload.get('T'),
            'E': payload.get('E')
        }
        
        trade_data = json.dumps(clean_trade)
        
        # 배치에 추가
        self.trade_batch.append((redis_key, trade_data))
        
        # 배치 크기에 도달하거나 일정 시간이 지나면 업로드
        if (len(self.trade_batch) >= self.batch_size or 
            current_time - self.last_batch_time >= self.batch_interval):
            await self._flush_trade_batch()
    
    async def _flush_trade_batch(self):
        """거래 데이터 배치 업로드"""
        if not self.trade_batch:
            return
        
        try:
            # Redis pipeline 사용으로 성능 최적화
            pipe = self.redis_uploader.redis_client.pipeline()
            
            for redis_key, trade_data in self.trade_batch:
                pipe.lpush(redis_key, trade_data)
                pipe.ltrim(redis_key, 0, self.trade_list_max_len - 1)
            
            pipe.execute()
            
            self.logger.debug(f"{len(self.trade_batch)}개의 거래 데이터를 배치 업로드했습니다.")
            
        except Exception as e:
            self.logger.error(f"거래 데이터 배치 업로드 실패: {e}")
        finally:
            self.trade_batch.clear()
            self.last_batch_time = time.time()
    
    def get_performance_stats(self) -> Dict[str, Any]:
        """성능 통계 조회"""
        uptime = time.time() - self.start_time
        processing_rate = self.processed_count / uptime if uptime > 0 else 0
        error_rate = self.error_count / max(self.processed_count, 1) * 100
        
        return {
            "processed_count": self.processed_count,
            "error_count": self.error_count,
            "uptime_seconds": uptime,
            "processing_rate_per_second": round(processing_rate, 2),
            "error_rate_percent": round(error_rate, 2),
            "batch_pending": len(self.trade_batch),
            "redis_connected": self.redis_uploader.is_connected()
        }

# 통합 클라이언트
class EnhancedBinanceWebSocketClient:
    """강화된 통합 WebSocket 클라이언트"""
    
    def __init__(self, symbols: List[str] = None):
        self.symbols = symbols or ["btcusdt"]
        self.logger = get_logger(__name__)
        
        # 스트림 설정
        self.streams = self._build_streams()
        
        # 컴포넌트 초기화
        self.data_handler = DataHandler()
        self.ws_client = WebSocketClient(
            streams=self.streams,
            on_message_callback=self.data_handler.handle_and_upload_message
        )
        
        self.logger.info(f"강화된 WebSocket 클라이언트 초기화 완료: {len(self.symbols)}개 심볼")
    
    def _build_streams(self) -> List[str]:
        """구독할 스트림 목록 생성"""
        streams = []
        for symbol in self.symbols:
            symbol_lower = symbol.lower()
            streams.extend([
                f"{symbol_lower}@kline_1m",
                f"{symbol_lower}@depth5@100ms",
                f"{symbol_lower}@aggTrade"
            ])
        return streams
    
    def start(self):
        """클라이언트 시작"""
        self.logger.info("강화된 WebSocket 클라이언트를 시작합니다.")
        self.ws_client.start()
    
    def stop(self):
        """클라이언트 중지"""
        self.logger.info("강화된 WebSocket 클라이언트를 중지합니다.")
        self.ws_client.stop()
        
        # 남은 배치 데이터 처리
        asyncio.run(self.data_handler._flush_trade_batch())
    
    def get_comprehensive_stats(self) -> Dict[str, Any]:
        """종합 통계 조회"""
        ws_stats = self.ws_client.get_connection_stats()
        data_stats = self.data_handler.get_performance_stats()
        
        return {
            "websocket": ws_stats,
            "data_processing": data_stats,
            "symbols": self.symbols,
            "streams": self.streams
        }

# 사용 예제
if __name__ == '__main__':
    import signal
    import sys
    
    def signal_handler(sig, frame):
        print('\nWebSocket 클라이언트를 중지합니다...')
        client.stop()
        sys.exit(0)
    
    # SIGINT 핸들러 등록
    signal.signal(signal.SIGINT, signal_handler)
    
    # 클라이언트 시작
    client = EnhancedBinanceWebSocketClient(["BTCUSDT"])

    print("WebSocket 클라이언트 시작...")
    print("Ctrl+C로 중지할 수 있습니다.")
    
    try:
        # 통계 모니터링 스레드
        import threading
        def monitor_stats():
            while True:
                time.sleep(30)  # 30초마다 통계 출력
                stats = client.get_comprehensive_stats()
                ws_stats = stats['websocket']
                data_stats = stats['data_processing']
                
                print(f"\n=== 성능 통계 (30초마다) ===")
                print(f"WebSocket: 연결됨={ws_stats['is_connected']}, 메시지={ws_stats['total_messages']}, 재연결={ws_stats['reconnect_count']}")
                print(f"데이터 처리: 처리율={data_stats['processing_rate_per_second']}/초, 오류율={data_stats['error_rate_percent']}%")
        
        monitor_thread = threading.Thread(target=monitor_stats, daemon=True)
        monitor_thread.start()
        
        # 클라이언트 실행
        client.start()
        
    except Exception as e:
        print(f"오류 발생: {e}")
        client.stop()