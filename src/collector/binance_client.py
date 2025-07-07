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
    """Handles communication with the Binance Futures API (REST).
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
            self.logger.error(f"Error requesting {endpoint}: {e}")
            return None

    def get_ticker_price(self, symbol: str):
        self.logger.info(f"Fetching ticker price for {symbol}")
        return self._send_request(self.BASE_URL, "/fapi/v2/ticker/price", {"symbol": symbol})

    def get_historical_klines(self, symbol: str, interval: str, start_date: str, end_date: str):
        self.logger.info(f"Fetching historical klines for {symbol} from {start_date} to {end_date}")
        start_ts = int(datetime.datetime.strptime(start_date, '%Y-%m-%d').timestamp() * 1000)
        end_ts = int(datetime.datetime.strptime(end_date, '%Y-%m-%d').timestamp() * 1000)
        
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
            start_ts = klines[-1][0] + 1 # Next start time is after the last kline's open time
        
        return all_klines

    def get_funding_rate(self, symbol: str, limit: int = 100):
        self.logger.info(f"Fetching funding rate for {symbol}")
        return self._send_request(self.BASE_URL, "/fapi/v1/fundingRate", {"symbol": symbol, "limit": limit})

    def get_open_interest(self, symbol: str):
        self.logger.info(f"Fetching open interest for {symbol}")
        return self._send_request(self.BASE_URL, "/fapi/v1/openInterest", {"symbol": symbol})

    def get_mark_price(self, symbol: str):
        self.logger.info(f"Fetching mark price for {symbol}")
        return self._send_request(self.BASE_URL, "/fapi/v1/premiumIndex", {"symbol": symbol})

    def get_long_short_ratio(self, symbol: str, period: str, limit: int = 500):
        self.logger.info(f"Fetching long/short ratio for {symbol} ({period})")
        return self._send_request(self.DATA_URL, "/globalLongShortAccountRatio", {
            "symbol": symbol, 
            "period": period, 
            "limit": limit
        })

    def get_force_orders(self, symbol: str = None, limit: int = 100):
        self.logger.info(f"Fetching force orders for {symbol or 'all symbols'}")
        params = {"limit": limit}
        if symbol:
            params["symbol"] = symbol
        return self._send_request(self.DATA_URL, "/allForceOrders", params)

    def get_order_book_depth(self, symbol: str, limit: int = 100):
        self.logger.info(f"Fetching order book depth for {symbol}")
        return self._send_request(self.BASE_URL, "/fapi/v1/depth", {"symbol": symbol, "limit": limit})
