# indicator_calculator.py
import pandas as pd
import ta

def calculate_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["ema_9"] = ta.trend.ema_indicator(df["close"], window=9)
    df["ema_21"] = ta.trend.ema_indicator(df["close"], window=21)
    df["rsi_14"] = ta.momentum.rsi(df["close"], window=14)

    macd = ta.trend.macd(df["close"])
    df["macd"] = macd.macd()
    df["macd_signal"] = macd.macd_signal()
    df["macd_hist"] = macd.macd_diff()

    df["bb_bbm"] = ta.volatility.bollinger_mavg(df["close"])
    df["bb_bbh"] = ta.volatility.bollinger_hband(df["close"])
    df["bb_bbl"] = ta.volatility.bollinger_lband(df["close"])

    df["atr"] = ta.volatility.average_true_range(df["high"], df["low"], df["close"])
    df["adx"] = ta.trend.adx(df["high"], df["low"], df["close"])
    df["obv"] = ta.volume.on_balance_volume(df["close"], df["volume"])

    return df.dropna()
