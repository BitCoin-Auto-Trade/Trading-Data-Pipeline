import os
import time
import hmac
import hashlib
import datetime
from urllib.parse import urlencode
import requests
import pandas as pd
from dotenv import load_dotenv
from src.utils.logger import get_logger

load_dotenv()

class BinanceClient:
    """바이낸스 선물 API (REST)와의 통신을 처리합니다.
    """
    BASE_URL = "https://fapi.binance.com"
    DATA_URL = "https://fapi.binance.com/futures/data"

    def __init__(self):
        self.api_key = os.getenv("BINANCE_API_KEY")
        self.api_secret = os.getenv("BINANCE_API_SECRET")
        self.logger = get_logger(__name__)

    def _get_signature(self, params):
        return hmac.new(
            self.api_secret.encode("utf-8"),
            urlencode(params).encode("utf-8"),
            hashlib.sha256
        ).hexdigest()

    def _send_request(self, base_url, endpoint, params=None, signed=False):
        if params is None:
            params = {}
        
        if signed:
            params["timestamp"] = int(time.time() * 1000)
            params["signature"] = self._get_signature(params)

        headers = {"X-MBX-APIKEY": self.api_key} if self.api_key else {}
        
        try:
            response = requests.get(f"{base_url}{endpoint}", params=params, headers=headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            self.logger.error(f"{endpoint} 요청 중 오류 발생: {e}")
            return None

    def get_ticker_price(self, symbol: str):
        self.logger.info(f"{symbol}의 티커 가격을 가져옵니다.")
        return self._send_request(self.BASE_URL, "/fapi/v2/ticker/price", {"symbol": symbol})

    def get_historical_klines(self, symbol: str, interval: str, start_date: str, end_date: str):
        self.logger.info(f"{start_date}부터 {end_date}까지 {symbol}의 과거 kline을 가져옵니다.")
        start_ts = int(datetime.datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S').timestamp() * 1000)
        end_ts = int(datetime.datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S').timestamp() * 1000)
        
        all_klines = []
        while start_ts < end_ts:
            klines = self._send_request(self.BASE_URL, "/fapi/v1/klines", {
                "symbol": symbol, 
                "interval": interval, 
                "startTime": start_ts, 
                "endTime": end_ts, 
                "limit": 1500
            })
            if not klines:
                break
            all_klines.extend(klines)
            start_ts = klines[-1][0] + 1 # 다음 시작 시간은 마지막 kline의 시작 시간 이후입니다.
        
        return all_klines

    def get_funding_rate(self, symbol: str, limit: int = 100):
        self.logger.info(f"{symbol}의 펀딩 비율을 가져옵니다.")
        return self._send_request(self.BASE_URL, "/fapi/v1/fundingRate", {"symbol": symbol, "limit": limit})

    def get_open_interest(self, symbol: str):
        self.logger.info(f"{symbol}의 미결제 약정을 가져옵니다.")
        return self._send_request(self.BASE_URL, "/fapi/v1/openInterest", {"symbol": symbol})

    def get_mark_price(self, symbol: str):
        self.logger.info(f"{symbol}의 마크 가격을 가져옵니다.")
        return self._send_request(self.BASE_URL, "/fapi/v1/premiumIndex", {"symbol": symbol})

    def get_long_short_ratio(self, symbol: str, period: str, limit: int = 500):
        self.logger.info(f"{symbol}의 롱/숏 비율을 가져옵니다 ({period})")
        return self._send_request(self.DATA_URL, "/globalLongShortAccountRatio", {
            "symbol": symbol, 
            "period": period, 
            "limit": limit
        })

    def get_force_orders(self, symbol: str = None, limit: int = 100):
        self.logger.info(f"{symbol or '모든 심볼'}의 강제 주문을 가져옵니다.")
        params = {"limit": limit}
        if symbol:
            params["symbol"] = symbol
        return self._send_request(self.DATA_URL, "/allForceOrders", params)

    def get_order_book_depth(self, symbol: str, limit: int = 100):
        self.logger.info(f"{symbol}의 오더북 깊이를 가져옵니다.")
        return self._send_request(self.BASE_URL, "/fapi/v1/depth", {"symbol": symbol, "limit": limit})
