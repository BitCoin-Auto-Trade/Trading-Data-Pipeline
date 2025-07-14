"""
바이낸스 선물 API 클라이언트
"""
import os
import time
import hmac
import hashlib
import datetime
from urllib.parse import urlencode
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from typing import Optional, List, Dict, Any
from dotenv import load_dotenv
from src.utils.logger import get_logger

load_dotenv()

class BinanceClient:
    """바이낸스 선물 API 클라이언트"""
    
    BASE_URL = "https://fapi.binance.com"
    DATA_URL = "https://fapi.binance.com/futures/data"
    
    def __init__(self):
        self.api_key = os.getenv("BINANCE_API_KEY")
        self.api_secret = os.getenv("BINANCE_API_SECRET")
        self.logger = get_logger(__name__)
        
        # 재시도 설정
        self.max_retries = 3
        self.retry_delay = 1.0  # 초기 지연 시간
        self.backoff_factor = 2.0  # 지수 백오프 계수
        
        # 레이트 리미트 관리
        self.rate_limit_buffer = 0.1  # 100ms 버퍼
        self.last_request_time = 0
        self.request_count = 0
        self.rate_limit_reset_time = 0
        
        # 서버 시간 동기화
        self.server_time_offset = 0
        self.last_time_sync = 0
        self.time_sync_interval = 3600  # 1시간마다 동기화
        
        # 성능 모니터링
        self.total_requests = 0
        self.failed_requests = 0
        self.retry_count = 0
        
        # HTTP 세션 설정 (연결 재사용)
        self.session = self._create_session()
        
        # 초기 서버 시간 동기화
        self._sync_server_time()
    
    def _create_session(self) -> requests.Session:
        """재시도 로직이 있는 HTTP 세션 생성"""
        session = requests.Session()
        
        # 재시도 전략 설정
        retry_strategy = Retry(
            total=self.max_retries,
            backoff_factor=self.backoff_factor,
            status_forcelist=[408, 429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "POST", "PUT", "DELETE", "OPTIONS", "TRACE"]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=10, pool_maxsize=20)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # 기본 헤더 설정
        session.headers.update({
            'User-Agent': 'Binance-Python-Client/1.0',
            'Accept': 'application/json',
            'Accept-Encoding': 'gzip, deflate'
        })
        
        return session
    
    def _sync_server_time(self):
        """바이낸스 서버 시간과 동기화"""
        try:
            current_time = time.time()
            if current_time - self.last_time_sync < self.time_sync_interval:
                return  # 아직 동기화할 시간이 아님
            
            response = self.session.get(f"{self.BASE_URL}/fapi/v1/time", timeout=10)
            if response.status_code == 200:
                server_time = response.json()['serverTime']
                local_time = int(time.time() * 1000)
                self.server_time_offset = server_time - local_time
                self.last_time_sync = current_time
                self.logger.debug(f"서버 시간 동기화 완료. 오프셋: {self.server_time_offset}ms")
            else:
                self.logger.warning(f"서버 시간 동기화 실패: {response.status_code}")
                
        except Exception as e:
            self.logger.warning(f"서버 시간 동기화 중 오류: {e}")
    
    def _get_server_timestamp(self) -> int:
        """동기화된 서버 타임스탬프 반환"""
        self._sync_server_time()  # 필요시 재동기화
        local_time = int(time.time() * 1000)
        return local_time + self.server_time_offset
    
    def _enforce_rate_limit(self):
        """레이트 리미트 준수"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        if time_since_last < self.rate_limit_buffer:
            sleep_time = self.rate_limit_buffer - time_since_last
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()
    
    def _get_signature(self, params: dict) -> str:
        """API 서명 생성"""
        if not self.api_secret:
            raise ValueError("API Secret이 설정되지 않았습니다")
        
        return hmac.new(
            self.api_secret.encode("utf-8"),
            urlencode(params).encode("utf-8"),
            hashlib.sha256
        ).hexdigest()
    
    def _validate_response(self, response: requests.Response, endpoint: str) -> bool:
        """응답 유효성 검증"""
        if response.status_code == 200:
            try:
                data = response.json()
                if isinstance(data, dict) and 'code' in data and data['code'] != 200:
                    self.logger.error(f"API 오류 응답 ({endpoint}): {data}")
                    return False
                return True
            except ValueError:
                self.logger.error(f"JSON 파싱 실패 ({endpoint})")
                return False
        elif response.status_code == 429:
            self.logger.warning(f"레이트 리미트 도달 ({endpoint})")
            return False
        elif response.status_code >= 500:
            self.logger.warning(f"서버 오류 ({endpoint}): {response.status_code}")
            return False
        else:
            self.logger.error(f"HTTP 오류 ({endpoint}): {response.status_code}")
            return False
    
    def _send_request(self, base_url: str, endpoint: str, params: Optional[dict] = None, 
                     signed: bool = False, timeout: int = 30) -> Optional[Any]:
        """재시도 로직이 있는 안전한 요청"""
        if params is None:
            params = {}
        
        self.total_requests += 1
        
        for attempt in range(self.max_retries + 1):
            try:
                # 레이트 리미트 준수
                self._enforce_rate_limit()
                
                # 서명된 요청 처리
                if signed:
                    if not self.api_key:
                        raise ValueError("API Key가 설정되지 않았습니다")
                    
                    params["timestamp"] = self._get_server_timestamp()
                    params["signature"] = self._get_signature(params)
                
                # 헤더 설정
                headers = {}
                if self.api_key:
                    headers["X-MBX-APIKEY"] = self.api_key
                
                # 요청 실행
                url = f"{base_url}{endpoint}"
                self.logger.debug(f"API 요청: {url} (시도 {attempt + 1}/{self.max_retries + 1})")
                
                response = self.session.get(url, params=params, headers=headers, timeout=timeout)
                
                # 응답 검증
                if self._validate_response(response, endpoint):
                    return response.json()
                
                # 재시도가 필요한 경우
                if attempt < self.max_retries:
                    self.retry_count += 1
                    delay = self.retry_delay * (self.backoff_factor ** attempt)
                    self.logger.warning(f"요청 실패. {delay}초 후 재시도: {endpoint}")
                    time.sleep(delay)
                else:
                    self.failed_requests += 1
                    self.logger.error(f"최대 재시도 횟수 초과: {endpoint}")
                    return None
                    
            except requests.exceptions.Timeout:
                self.logger.warning(f"요청 타임아웃 ({endpoint}) - 시도 {attempt + 1}")
                if attempt == self.max_retries:
                    self.failed_requests += 1
                    return None
                    
            except requests.exceptions.ConnectionError:
                self.logger.warning(f"연결 오류 ({endpoint}) - 시도 {attempt + 1}")
                if attempt == self.max_retries:
                    self.failed_requests += 1
                    return None
                    
            except Exception as e:
                self.logger.error(f"예상치 못한 오류 ({endpoint}): {e}")
                if attempt == self.max_retries:
                    self.failed_requests += 1
                    return None
        
        return None
    
    def get_ticker_price(self, symbol: str) -> Optional[Dict]:
        """티커 가격 조회"""
        self.logger.debug(f"{symbol}의 티커 가격을 가져옵니다.")
        
        result = self._send_request(
            self.BASE_URL, 
            "/fapi/v2/ticker/price", 
            {"symbol": symbol.upper()}
        )
        
        if result and 'price' in result:
            self.logger.debug(f"{symbol} 가격: {result['price']}")
        
        return result
    
    def get_historical_klines(self, symbol: str, interval: str, start_date: str, 
                            end_date: str, limit: int = 1500) -> List[List]:
        """과거 kline 데이터 조회 (페이지네이션 지원)"""
        self.logger.info(f"{start_date}부터 {end_date}까지 {symbol}의 과거 kline을 가져옵니다.")
        
        try:
            start_ts = int(datetime.datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S').timestamp() * 1000)
            end_ts = int(datetime.datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S').timestamp() * 1000)
        except ValueError as e:
            self.logger.error(f"날짜 형식 오류: {e}")
            return []
        
        all_klines = []
        current_start = start_ts
        
        # 안전 장치: 무한 루프 방지
        max_iterations = 100
        iteration = 0
        
        while current_start < end_ts and iteration < max_iterations:
            iteration += 1
            
            params = {
                "symbol": symbol.upper(),
                "interval": interval,
                "startTime": current_start,
                "endTime": end_ts,
                "limit": min(limit, 1500)  # 바이낸스 최대 제한
            }
            
            klines = self._send_request(self.BASE_URL, "/fapi/v1/klines", params)
            
            if not klines or len(klines) == 0:
                self.logger.warning(f"더 이상 가져올 kline 데이터가 없습니다. (반복: {iteration})")
                break
            
            # 데이터 유효성 검증
            valid_klines = []
            for kline in klines:
                if isinstance(kline, list) and len(kline) >= 12:
                    valid_klines.append(kline[:12])
                else:
                    self.logger.warning(f"유효하지 않은 kline 데이터: {kline}")
            
            all_klines.extend(valid_klines)
            
            # 다음 시작 시간 설정 (마지막 kline의 종료 시간 + 1ms)
            if valid_klines:
                current_start = valid_klines[-1][6] + 1  # Close time + 1ms
            else:
                break
            
            self.logger.debug(f"배치 {iteration}: {len(valid_klines)}개 kline 수집 완료")
        
        self.logger.info(f"총 {len(all_klines)}개의 kline 데이터를 수집했습니다.")
        return all_klines
    
    def get_funding_rate(self, symbol: str, limit: int = 100) -> Optional[List[Dict]]:
        """펀딩 비율 조회"""
        self.logger.debug(f"{symbol}의 펀딩 비율을 가져옵니다.")
        
        params = {
            "symbol": symbol.upper(),
            "limit": min(limit, 1000)  # 최대 제한
        }
        
        result = self._send_request(self.BASE_URL, "/fapi/v1/fundingRate", params)
        
        if result and isinstance(result, list):
            self.logger.debug(f"{symbol} 펀딩 비율 {len(result)}개 조회 완료")
        
        return result
    
    def get_open_interest(self, symbol: str) -> Optional[Dict]:
        """미결제 약정 조회"""
        self.logger.debug(f"{symbol}의 미결제 약정을 가져옵니다.")
        
        result = self._send_request(
            self.BASE_URL, 
            "/fapi/v1/openInterest", 
            {"symbol": symbol.upper()}
        )
        
        if result and 'openInterest' in result:
            self.logger.debug(f"{symbol} 미결제 약정: {result['openInterest']}")
        
        return result
    
    def get_mark_price(self, symbol: str) -> Optional[Dict]:
        """마크 가격 조회"""
        self.logger.debug(f"{symbol}의 마크 가격을 가져옵니다.")
        
        result = self._send_request(
            self.BASE_URL, 
            "/fapi/v1/premiumIndex", 
            {"symbol": symbol.upper()}
        )
        
        return result
    
    def get_long_short_ratio(self, symbol: str, period: str, limit: int = 500) -> Optional[List[Dict]]:
        """롱/숏 비율 조회"""
        self.logger.debug(f"{symbol}의 롱/숏 비율을 가져옵니다 ({period})")
        
        params = {
            "symbol": symbol.upper(),
            "period": period,
            "limit": min(limit, 500)
        }
        
        result = self._send_request(self.DATA_URL, "/globalLongShortAccountRatio", params)
        
        return result
    
    def get_force_orders(self, symbol: Optional[str] = None, limit: int = 100) -> Optional[List[Dict]]:
        """강제 청산 주문 조회"""
        symbol_name = symbol or "모든 심볼"
        self.logger.debug(f"{symbol_name}의 강제 주문을 가져옵니다.")
        
        params = {"limit": min(limit, 1000)}
        if symbol:
            params["symbol"] = symbol.upper()
        
        result = self._send_request(self.DATA_URL, "/allForceOrders", params)
        
        return result
    
    def get_order_book_depth(self, symbol: str, limit: int = 100) -> Optional[Dict]:
        """오더북 깊이 조회"""
        self.logger.debug(f"{symbol}의 오더북 깊이를 가져옵니다.")
        
        params = {
            "symbol": symbol.upper(),
            "limit": min(limit, 1000)
        }
        
        result = self._send_request(self.BASE_URL, "/fapi/v1/depth", params)
        
        # 오더북 데이터 유효성 검증
        if result and 'bids' in result and 'asks' in result:
            if not isinstance(result['bids'], list) or not isinstance(result['asks'], list):
                self.logger.warning(f"유효하지 않은 오더북 데이터 형식: {symbol}")
                return None
            
            self.logger.debug(f"{symbol} 오더북: {len(result['bids'])}개 매수, {len(result['asks'])}개 매도")
        
        return result
    
    def health_check(self) -> bool:
        """API 연결 상태 확인"""
        try:
            result = self._send_request(self.BASE_URL, "/fapi/v1/ping", timeout=10)
            is_healthy = result is not None
            
            if is_healthy:
                self.logger.debug("바이낸스 API 연결 상태 양호")
            else:
                self.logger.warning("바이낸스 API 연결 상태 불량")
            
            return is_healthy
            
        except Exception as e:
            self.logger.error(f"헬스 체크 중 오류: {e}")
            return False
    
    def get_performance_stats(self) -> Dict[str, Any]:
        """성능 통계 조회"""
        success_rate = (
            (self.total_requests - self.failed_requests) / self.total_requests * 100
            if self.total_requests > 0 else 0
        )
        
        return {
            "total_requests": self.total_requests,
            "failed_requests": self.failed_requests,
            "retry_count": self.retry_count,
            "success_rate": round(success_rate, 2),
            "server_time_offset": self.server_time_offset,
            "last_time_sync": datetime.datetime.fromtimestamp(self.last_time_sync).isoformat() if self.last_time_sync else None
        }
    
    def reset_stats(self):
        """통계 초기화"""
        self.total_requests = 0
        self.failed_requests = 0
        self.retry_count = 0
        self.logger.info("성능 통계가 초기화되었습니다.")
    
    def close(self):
        """리소스 정리"""
        if self.session:
            self.session.close()
            self.logger.info("HTTP 세션이 종료되었습니다.")

# 사용 예제 및 테스트
if __name__ == '__main__':
    client = ResilientBinanceClient()
    
    # 헬스 체크
    if client.health_check():
        print("API 연결 성공")
        
        # 성능 테스트
        print("\n=== 성능 테스트 ===")
        start_time = time.time()
        
        # 티커 가격 조회
        ticker = client.get_ticker_price("BTCUSDT")
        if ticker:
            print(f"BTC 가격: ${ticker['price']}")
        
        # 과거 데이터 조회 (최근 10분)
        end_time = datetime.datetime.now()
        start_time_str = (end_time - datetime.timedelta(minutes=10)).strftime('%Y-%m-%d %H:%M:%S')
        end_time_str = end_time.strftime('%Y-%m-%d %H:%M:%S')
        
        klines = client.get_historical_klines("BTCUSDT", "1m", start_time_str, end_time_str)
        print(f"과거 데이터: {len(klines)}개 캔들")
        
        # 성능 통계
        elapsed = time.time() - start_time
        stats = client.get_performance_stats()
        print(f"\n=== 성능 통계 ===")
        print(f"총 소요 시간: {elapsed:.2f}초")
        print(f"총 요청 수: {stats['total_requests']}")
        print(f"실패 요청 수: {stats['failed_requests']}")
        print(f"성공률: {stats['success_rate']}%")
        print(f"재시도 횟수: {stats['retry_count']}")
        
    else:
        print("❌ API 연결 실패")
    
    client.close()