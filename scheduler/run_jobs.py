from apscheduler.schedulers.background import BackgroundScheduler
import time
import datetime
import pandas as pd
from src.collector.binance_client import BinanceClient
from src.formatter.indicator_calculator import IndicatorCalculator
from src.uploader.postgres_uploader import PostgresUploader
from src.utils.logger import get_logger

logger = get_logger("scheduler")

def fetch_and_process_binance_data():
    """Fetches, processes, and logs Binance data for BTC/USDT, then uploads to PostgreSQL.
    """
    logger.info("Starting Binance data processing and upload job.")
    
    collector = BinanceClient()
    formatter = IndicatorCalculator()
    uploader = PostgresUploader()
    symbol = "BTCUSDT"

    if not uploader.is_connected():
        logger.error("PostgreSQL uploader is not connected. Skipping data upload.")
        return

    # --- 1. Fetch historical and new kline data ---
    # Fetch last 100 klines from the database to have enough data for indicator calculation
    historical_klines_df = uploader.get_historical_klines(symbol, limit=100)

    # Determine the start time for fetching new data to avoid duplicates
    if not historical_klines_df.empty:
        last_timestamp = historical_klines_df.index.max().to_pydatetime()
        start_time = (last_timestamp + datetime.timedelta(minutes=1)).strftime('%Y-%m-%d %H:%M:%S')
    else:
        # If the database is empty, fetch the last 100 minutes of data
        start_time = (datetime.datetime.now() - datetime.timedelta(minutes=100)).strftime('%Y-%m-%d %H:%M:%S')
    
    end_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    new_raw_klines = collector.get_historical_klines(symbol, "1m", start_time, end_time)

    # --- 2. Formatter: Process kline data and calculate indicators ---
    if new_raw_klines:
        new_klines_df = formatter.format_klines(new_raw_klines)
        
        if new_klines_df is not None and not new_klines_df.empty:
            # Combine historical data with new data
            combined_df = pd.concat([historical_klines_df, new_klines_df])
            # Remove duplicates, keeping the latest entry
            combined_df = combined_df[~combined_df.index.duplicated(keep='last')]
            
            # Calculate indicators on the combined dataset
            combined_df = formatter.calculate_ema(combined_df, period=20)
            combined_df = formatter.calculate_rsi(combined_df, period=14)
            combined_df = formatter.calculate_macd(combined_df)
            
            # Filter for only the new rows to be uploaded
            rows_to_upload = combined_df[combined_df.index.isin(new_klines_df.index)]

            # --- 3. Uploader: Upload processed data to PostgreSQL ---
            for index, row in rows_to_upload.iterrows():
                kline_data = {
                    'timestamp': index.to_pydatetime(),
                    'symbol': symbol,
                    'open': row['open'],
                    'high': row['high'],
                    'low': row['low'],
                    'close': row['close'],
                    'volume': row['volume'],
                    'ema_20': row.get('ema_20'),
                    'rsi_14': row.get('rsi_14'),
                    'macd': row.get('macd'),
                    'macd_signal': row.get('macd_signal'),
                    'macd_hist': row.get('macd_hist')
                }
                uploader.upload_kline_data(kline_data)
            logger.info(f"Uploaded {len(rows_to_upload)} new kline records to PostgreSQL.")
        else:
            logger.warning("Kline DataFrame is empty after formatting. Skipping kline upload.")
    else:
        logger.info("No new kline data fetched from Binance.")

    # --- Funding Rate and Open Interest (unchanged) ---
    raw_funding_rate = collector.get_funding_rate(symbol, limit=1)
    if raw_funding_rate and len(raw_funding_rate) > 0:
        latest_funding_rate = raw_funding_rate[0]
        funding_rate_data = {
            'timestamp': datetime.datetime.fromtimestamp(latest_funding_rate['fundingTime'] / 1000),
            'symbol': latest_funding_rate['symbol'],
            'funding_rate': float(latest_funding_rate['fundingRate'])
        }
        uploader.upload_funding_rate(funding_rate_data)
        logger.info(f"Uploaded latest funding rate for {symbol} to PostgreSQL.")

    raw_open_interest = collector.get_open_interest(symbol)
    if raw_open_interest:
        open_interest_data = {
            'timestamp': datetime.datetime.fromtimestamp(raw_open_interest['time'] / 1000),
            'symbol': raw_open_interest['symbol'],
            'open_interest': float(raw_open_interest['openInterest'])
        }
        uploader.upload_open_interest(open_interest_data)
        logger.info(f"Uploaded latest open interest for {symbol} to PostgreSQL.")

    logger.info("Binance data processing and upload job finished.")

def start_scheduler():
    """Initializes and starts the job scheduler.
    """
    scheduler = BackgroundScheduler()
    scheduler.add_job(fetch_and_process_binance_data, 'interval', minutes=1)
    scheduler.start()
    logger.info("Scheduler started. Fetching data every 1 minute.")

    try:
        while True:
            time.sleep(10)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        logger.info("Scheduler stopped.")

if __name__ == "__main__":
    start_scheduler()
