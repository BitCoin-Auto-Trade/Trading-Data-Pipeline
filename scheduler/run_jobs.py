from apscheduler.schedulers.background import BackgroundScheduler
import time
from src.collector.binance_client import BinanceClient
from src.formatter.indicator_calculator import IndicatorCalculator
from src.utils.logger import get_logger
from src.utils.helper import safe_get

logger = get_logger("scheduler")

def fetch_and_process_binance_data():
    """Fetches, processes, and logs Binance data for BTC/USDT.
    """
    logger.info("Starting Binance data processing job.")
    
    # 1. Collector: Fetch raw data
    collector = BinanceClient()
    symbol = "BTCUSDT"
    
    raw_price = collector.get_ticker_price(symbol)
    raw_klines = collector.get_klines(symbol, "1h", limit=100)
    raw_funding_rate = collector.get_funding_rate(symbol, limit=1)
    raw_open_interest = collector.get_open_interest(symbol)

    # Log raw data for verification
    logger.info(f"Raw Price: {raw_price}")
    logger.info(f"Raw Funding Rate: {raw_funding_rate}")
    logger.info(f"Raw Open Interest: {raw_open_interest}")

    # 2. Formatter: Process kline data and calculate indicators
    if raw_klines:
        formatter = IndicatorCalculator()
        klines_df = formatter.format_klines(raw_klines)
        
        if klines_df is not None and not klines_df.empty:
            klines_df = formatter.calculate_ema(klines_df, period=20)
            klines_df = formatter.calculate_rsi(klines_df, period=14)
            
            latest_kline = klines_df.iloc[-1]
            logger.info(f"Processed Kline Data (latest):\n{latest_kline.to_string()}")
    else:
        logger.warning("Could not fetch kline data. Skipping formatting.")

    # 3. Uploader: (To be implemented)
    # For now, we just log the results.

    logger.info("Binance data processing job finished.")

def start_scheduler():
    """Initializes and starts the job scheduler.
    """
    scheduler = BackgroundScheduler()
    # Run the job every 1 minute
    scheduler.add_job(fetch_and_process_binance_data, 'interval', minutes=1)
    scheduler.start()
    logger.info("Scheduler started. Fetching data every 1 minute.")

    try:
        # Keep the main thread alive
        while True:
            time.sleep(10)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        logger.info("Scheduler stopped.")

if __name__ == "__main__":
    start_scheduler()
