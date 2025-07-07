import os
import time
import hmac
import hashlib
from urllib.parse import urlencode
import requests
from dotenv import load_dotenv
from src.utils.logger import get_logger

load_dotenv()

class BinanceClient:
    """Handles communication with the Binance Futures API.
    """
    BASE_URL = "https://fapi.binance.com"

    def __init__(self):
        self.api_key = os.getenv("BINANCE_API_KEY")
        self.api_secret = os.getenv("BINANCE_API_SECRET")
        self.logger = get_logger(__name__)

    def _get_signature(self, params):
        """Generates the HMAC SHA256 signature for signed requests.
        """
        return hmac.new(
            self.api_secret.encode("utf-8"),
            urlencode(params).encode("utf-8"),
            hashlib.sha256
        ).hexdigest()

    def _send_request(self, endpoint, params=None, signed=False):
        """Sends a request to the Binance API.
        """
        if params is None:
            params = {}
        
        if signed:
            params["timestamp"] = int(time.time() * 1000)
            params["signature"] = self._get_signature(params)

        headers = {"X-MBX-APIKEY": self.api_key} if self.api_key else {}
        
        try:
            response = requests.get(
                f"{self.BASE_URL}{endpoint}",
                params=params,
                headers=headers
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error requesting {endpoint}: {e}")
            return None

    def get_ticker_price(self, symbol: str):
        """Fetches the latest price for a specific symbol.
        """
        self.logger.info(f"Fetching ticker price for {symbol}")
        return self._send_request("/fapi/v2/ticker/price", {"symbol": symbol})

    def get_klines(self, symbol: str, interval: str, start_time=None, end_time=None, limit: int = 500):
        """Fetches raw Kline/Candlestick data.
        """
        self.logger.info(f"Fetching klines for {symbol} with interval {interval}")
        params = {"symbol": symbol, "interval": interval, "limit": limit}
        if start_time:
            params["startTime"] = start_time
        if end_time:
            params["endTime"] = end_time
        return self._send_request("/fapi/v1/klines", params)

    def get_funding_rate(self, symbol: str, limit: int = 100):
        """Fetches funding rate history.
        """
        self.logger.info(f"Fetching funding rate for {symbol}")
        return self._send_request("/fapi/v1/fundingRate", {"symbol": symbol, "limit": limit})

    def get_open_interest(self, symbol: str):
        """Fetches open interest for a specific symbol.
        """
        self.logger.info(f"Fetching open interest for {symbol}")
        return self._send_request("/fapi/v1/openInterest", {"symbol": symbol})