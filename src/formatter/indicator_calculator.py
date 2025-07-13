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

    def calculate_atr(self, klines_df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
        """Average True Range (ATR)를 계산합니다.
        """
        high_low = klines_df['high'] - klines_df['low']
        high_close = (klines_df['high'] - klines_df['close'].shift()).abs()
        low_close = (klines_df['low'] - klines_df['close'].shift()).abs()
        
        tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        
        klines_df[f'atr_{period}'] = tr.ewm(span=period, adjust=False).mean()
        
        self.logger.info(f"{len(klines_df)}개의 데이터 포인트에 대해 ATR({period})를 계산했습니다.")
        return klines_df

    def calculate_adx(self, klines_df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
        """Average Directional Index (ADX)를 계산합니다.
        """
        df = klines_df.copy()
        df['up_move'] = df['high'].diff()
        df['down_move'] = -df['low'].diff()
        
        df['plus_dm'] = 0.0
        df.loc[(df['up_move'] > df['down_move']) & (df['up_move'] > 0), 'plus_dm'] = df['up_move']
        
        df['minus_dm'] = 0.0
        df.loc[(df['down_move'] > df['up_move']) & (df['down_move'] > 0), 'minus_dm'] = df['down_move']
        
        tr = pd.concat([
            df['high'] - df['low'],
            (df['high'] - df['close'].shift()).abs(),
            (df['low'] - df['close'].shift()).abs()
        ], axis=1).max(axis=1)
        
        atr = tr.ewm(span=period, adjust=False).mean()
        
        plus_di = 100 * (df['plus_dm'].ewm(span=period, adjust=False).mean() / atr)
        minus_di = 100 * (df['minus_dm'].ewm(span=period, adjust=False).mean() / atr)
        
        dx = 100 * (abs(plus_di - minus_di) / (plus_di + minus_di))
        
        klines_df['adx'] = dx.ewm(span=period, adjust=False).mean()
        self.logger.info(f"{len(klines_df)}개의 데이터 포인트에 대해 ADX({period})를 계산했습니다.")
        return klines_df

    def calculate_sma(self, klines_df: pd.DataFrame, period: int) -> pd.DataFrame:
        """Simple Moving Average (SMA)를 계산합니다.
        """
        klines_df[f'sma_{period}'] = klines_df['close'].rolling(window=period).mean()
        self.logger.info(f"{len(klines_df)}개의 데이터 포인트에 대해 SMA({period})를 계산했습니다.")
        return klines_df

    def calculate_bollinger_bands(self, klines_df: pd.DataFrame, period: int = 20, std_dev: int = 2) -> pd.DataFrame:
        """Bollinger Bands를 계산합니다.
        """
        sma = klines_df['close'].rolling(window=period).mean()
        std = klines_df['close'].rolling(window=period).std()
        
        klines_df['bb_upper'] = sma + (std * std_dev)
        klines_df['bb_middle'] = sma
        klines_df['bb_lower'] = sma - (std * std_dev)
        
        self.logger.info(f"{len(klines_df)}개의 데이터 포인트에 대해 Bollinger Bands({period}, {std_dev})를 계산했습니다.")
        return klines_df

    def calculate_stochastic_oscillator(self, klines_df: pd.DataFrame, k_period: int = 14, d_period: int = 3) -> pd.DataFrame:
        """Stochastic Oscillator를 계산합니다.
        """
        low_min = klines_df['low'].rolling(window=k_period).min()
        high_max = klines_df['high'].rolling(window=k_period).max()
        
        klines_df['stoch_k'] = 100 * ((klines_df['close'] - low_min) / (high_max - low_min))
        klines_df['stoch_d'] = klines_df['stoch_k'].rolling(window=d_period).mean()
        
        self.logger.info(f"{len(klines_df)}개의 데이터 포인트에 대해 Stochastic Oscillator({k_period}, {d_period})를 계산했습니다.")
        return klines_df