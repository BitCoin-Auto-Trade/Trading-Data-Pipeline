from apscheduler.schedulers.background import BackgroundScheduler
import time
import datetime
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

    # --- 1. Collector: Fetch raw data ---
    # Fetch historical klines for the last 2 days (for testing historical data fetching)
    end_date = datetime.datetime.now()
    start_date = end_date - datetime.timedelta(days=2)
    raw_klines = collector.get_historical_klines(
        symbol, 
        "1h", 
        start_date.strftime('%Y-%m-%d'), 
        end_date.strftime('%Y-%m-%d')
    )

    raw_funding_rate = collector.get_funding_rate(symbol, limit=1)
    raw_open_interest = collector.get_open_interest(symbol)

    # --- 2. Formatter: Process kline data and calculate indicators ---
    if raw_klines:
        klines_df = formatter.format_klines(raw_klines)
        
        if klines_df is not None and not klines_df.empty:
            klines_df = formatter.calculate_ema(klines_df, period=20)
            klines_df = formatter.calculate_rsi(klines_df, period=14)
            klines_df = formatter.calculate_macd(klines_df)
            
            # --- 3. Uploader: Upload processed data to PostgreSQL ---
            # Upload each row of the DataFrame to klines_1m table
            for index, row in klines_df.iterrows():
                kline_data = {
                    'timestamp': index.to_pydatetime(), # Convert pandas timestamp to python datetime
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
            logger.info(f"Uploaded {len(klines_df)} kline records to PostgreSQL.")
        else:
            logger.warning("Could not fetch kline data or DataFrame is empty. Skipping kline upload.")
    else:
        logger.warning("No raw kline data fetched. Skipping kline processing and upload.")

    # Upload Funding Rate
    if raw_funding_rate and len(raw_funding_rate) > 0:
        latest_funding_rate = raw_funding_rate[0]
        funding_rate_data = {
            'timestamp': datetime.datetime.fromtimestamp(latest_funding_rate['fundingTime'] / 1000),
            'symbol': latest_funding_rate['symbol'],
            'funding_rate': float(latest_funding_rate['fundingRate'])
        }
        uploader.upload_funding_rate(funding_rate_data)
        logger.info(f"Uploaded latest funding rate for {symbol} to PostgreSQL.")
    else:
        logger.warning("No funding rate data fetched. Skipping funding rate upload.")

    # Upload Open Interest
    if raw_open_interest:
        open_interest_data = {
            'timestamp': datetime.datetime.fromtimestamp(raw_open_interest['time'] / 1000),
            'symbol': raw_open_interest['symbol'],
            'open_interest': float(raw_open_interest['openInterest'])
        }
        uploader.upload_open_interest(open_interest_data)
        logger.info(f"Uploaded latest open interest for {symbol} to PostgreSQL.")
    else:
        logger.warning("No open interest data fetched. Skipping open interest upload.")

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
