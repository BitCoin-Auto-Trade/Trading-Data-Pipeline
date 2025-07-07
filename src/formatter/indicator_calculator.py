import pandas as pd
from src.utils.logger import get_logger

class IndicatorCalculator:
    """Calculates technical indicators from raw kline data.
    """
    def __init__(self):
        self.logger = get_logger(__name__)

    def format_klines(self, raw_klines: list) -> pd.DataFrame | None:
        """Converts raw kline list to a formatted Pandas DataFrame.
        """
        if not raw_klines:
            self.logger.warning("Received empty raw klines data.")
            return None
        
        df = pd.DataFrame(raw_klines, columns=[
            'open_time', 'open', 'high', 'low', 'close', 'volume', 
            'close_time', 'quote_asset_volume', 'number_of_trades',
            'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'
        ])
        
        # Convert timestamp to datetime and set as index
        df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')
        df.set_index('open_time', inplace=True)

        # Convert relevant columns to numeric types
        numeric_cols = ['open', 'high', 'low', 'close', 'volume']
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        self.logger.info(f"Formatted {len(df)} klines into DataFrame.")
        return df

    def calculate_ema(self, klines_df: pd.DataFrame, period: int = 20) -> pd.DataFrame:
        """Calculates Exponential Moving Average (EMA).
        """
        klines_df[f'ema_{period}'] = klines_df['close'].ewm(span=period, adjust=False).mean()
        self.logger.info(f"Calculated EMA({period}) for {len(klines_df)} data points.")
        return klines_df

    def calculate_rsi(self, klines_df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
        """Calculates Relative Strength Index (RSI).
        """
        delta = klines_df['close'].diff()
        gain = (delta.where(delta > 0, 0)).ewm(span=period, adjust=False).mean()
        loss = (-delta.where(delta < 0, 0)).ewm(span=period, adjust=False).mean()
        
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        
        klines_df[f'rsi_{period}'] = rsi
        self.logger.info(f"Calculated RSI({period}) for {len(klines_df)} data points.")
        return klines_df
