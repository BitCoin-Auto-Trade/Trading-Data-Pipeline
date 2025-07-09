import pandas as pd
from src.utils.logger import get_logger

class IndicatorCalculator:
    """원시 kline 데이터로부터 기술적 지표를 계산합니다.
    """
    def __init__(self):
        self.logger = get_logger(__name__)

    def format_klines(self, raw_klines: list) -> pd.DataFrame | None:
        """원시 kline 목록을 형식화된 Pandas DataFrame으로 변환합니다.
        """
        if not raw_klines:
            self.logger.warning("비어 있는 원시 kline 데이터를 받았습니다.")
            return None
        
        df = pd.DataFrame(raw_klines, columns=[
            'open_time', 'open', 'high', 'low', 'close', 'volume', 
            'close_time', 'quote_asset_volume', 'number_of_trades',
            'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'
        ])
        
        df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')
        df.set_index('open_time', inplace=True)

        numeric_cols = ['open', 'high', 'low', 'close', 'volume']
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        self.logger.info(f"{len(df)}개의 kline을 DataFrame으로 형식화했습니다.")
        return df

    def calculate_ema(self, klines_df: pd.DataFrame, period: int) -> pd.DataFrame:
        """지수 이동 평균(EMA)을 계산합니다.
        """
        klines_df[f'ema_{period}'] = klines_df['close'].ewm(span=period, adjust=False).mean()
        self.logger.info(f"{len(klines_df)}개의 데이터 포인트에 대해 EMA({period})를 계산했습니다.")
        return klines_df

    def calculate_rsi(self, klines_df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
        """상대 강도 지수(RSI)를 계산합니다.
        """
        delta = klines_df['close'].diff()
        gain = (delta.where(delta > 0, 0)).ewm(span=period, adjust=False).mean()
        loss = (-delta.where(delta < 0, 0)).ewm(span=period, adjust=False).mean()
        
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        
        klines_df[f'rsi_{period}'] = rsi
        self.logger.info(f"{len(klines_df)}개의 데이터 포인트에 대해 RSI({period})를 계산했습니다.")
        return klines_df

    def calculate_macd(self, klines_df: pd.DataFrame, short_period: int = 12, long_period: int = 26, signal_period: int = 9) -> pd.DataFrame:
        """이동 평균 수렴 발산(MACD)을 계산합니다.
        """
        # 단기 및 장기 EMA 계산
        ema_short = klines_df['close'].ewm(span=short_period, adjust=False).mean()
        ema_long = klines_df['close'].ewm(span=long_period, adjust=False).mean()
        
        # MACD 라인 계산
        klines_df['macd'] = ema_short - ema_long
        
        # 신호선 계산
        klines_df['macd_signal'] = klines_df['macd'].ewm(span=signal_period, adjust=False).mean()
        
        # MACD 히스토그램 계산
        klines_df['macd_hist'] = klines_df['macd'] - klines_df['macd_signal']
        
        self.logger.info(f"{len(klines_df)}개의 데이터 포인트에 대해 MACD({short_period},{long_period},{signal_period})를 계산했습니다.")
        return klines_df