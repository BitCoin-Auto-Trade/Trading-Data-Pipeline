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
    """BTC/USDT에 대한 바이낸스 데이터를 가져오고, 처리하고, 기록한 다음 PostgreSQL에 업로드합니다.
    """
    logger.info("바이낸스 데이터 처리 및 업로드 작업을 시작합니다.")
    
    collector = BinanceClient()
    formatter = IndicatorCalculator()
    uploader = PostgresUploader()
    symbol = "BTCUSDT"

    if not uploader.is_connected():
        logger.error("PostgreSQL 업로더가 연결되지 않았습니다. 데이터 업로드를 건너뜁니다.")
        return

    # --- 1. 과거 및 신규 kline 데이터 가져오기 ---
    # 지표 계산에 충분한 데이터를 확보하기 위해 데이터베이스에서 마지막 100개의 kline을 가져옵니다.
    historical_klines_df = uploader.get_historical_klines(symbol, limit=100)

    # 중복을 피하기 위해 새 데이터 가져오기 시작 시간을 결정합니다.
    if not historical_klines_df.empty:
        last_timestamp = historical_klines_df.index.max().to_pydatetime()
        start_time = (last_timestamp + datetime.timedelta(minutes=1)).strftime('%Y-%m-%d %H:%M:%S')
    else:
        # 데이터베이스가 비어 있으면 마지막 100분 분량의 데이터를 가져옵니다.
        start_time = (datetime.datetime.now() - datetime.timedelta(minutes=100)).strftime('%Y-%m-%d %H:%M:%S')
    
    end_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    new_raw_klines = collector.get_historical_klines(symbol, "1m", start_time, end_time)

    # --- 2. 포맷터: kline 데이터를 처리하고 지표를 계산합니다 ---
    if new_raw_klines:
        new_klines_df = formatter.format_klines(new_raw_klines)
        
        if new_klines_df is not None and not new_klines_df.empty:
            # 과거 데이터와 새 데이터를 결합합니다.
            combined_df = pd.concat([historical_klines_df, new_klines_df])
            # 중복을 제거하고 최신 항목을 유지합니다.
            combined_df = combined_df[~combined_df.index.duplicated(keep='last')]
            
            # 결합된 데이터 세트에서 지표를 계산합니다.
            combined_df = formatter.calculate_ema(combined_df, period=20)
            combined_df = formatter.calculate_rsi(combined_df, period=14)
            combined_df = formatter.calculate_macd(combined_df)
            combined_df = formatter.calculate_atr(combined_df, period=14)
            
            # 업로드할 새 행만 필터링합니다.
            rows_to_upload = combined_df[combined_df.index.isin(new_klines_df.index)]

            # --- 3. 업로더: 처리된 데이터를 PostgreSQL에 업로드합니다 ---
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
                    'macd_hist': row.get('macd_hist'),
                    'atr_14': row.get('atr_14')
                }
                uploader.upload_kline_data(kline_data)
            logger.info(f"{len(rows_to_upload)}개의 새로운 kline 레코드를 PostgreSQL에 업로드했습니다.")
        else:
            logger.warning("포맷팅 후 Kline DataFrame이 비어 있습니다. kline 업로드를 건너뜁니다.")
    else:
        logger.info("바이낸스에서 가져온 새로운 kline 데이터가 없습니다.")

    # --- 펀딩 비율 및 미결제 약정 (변경 없음) ---
    raw_funding_rate = collector.get_funding_rate(symbol, limit=1)
    if raw_funding_rate and len(raw_funding_rate) > 0:
        latest_funding_rate = raw_funding_rate[0]
        funding_rate_data = {
            'timestamp': datetime.datetime.fromtimestamp(latest_funding_rate['fundingTime'] / 1000),
            'symbol': latest_funding_rate['symbol'],
            'funding_rate': float(latest_funding_rate['fundingRate'])
        }
        uploader.upload_funding_rate(funding_rate_data)
        logger.info(f"{symbol}에 대한 최신 펀딩 비율을 PostgreSQL에 업로드했습니다.")

    raw_open_interest = collector.get_open_interest(symbol)
    if raw_open_interest:
        open_interest_data = {
            'timestamp': datetime.datetime.fromtimestamp(raw_open_interest['time'] / 1000),
            'symbol': raw_open_interest['symbol'],
            'open_interest': float(raw_open_interest['openInterest'])
        }
        uploader.upload_open_interest(open_interest_data)
        logger.info(f"{symbol}에 대한 최신 미결제 약정을 PostgreSQL에 업로드했습니다.")

    logger.info("바이낸스 데이터 처리 및 업로드 작업이 완료되었습니다.")

def start_scheduler():
    """작업 스케줄러를 초기화하고 시작합니다.
    """
    scheduler = BackgroundScheduler()
    scheduler.add_job(fetch_and_process_binance_data, 'cron', minute='*')
    scheduler.start()
    logger.info("스케줄러가 시작되었습니다. 매 분마다 데이터를 가져옵니다.")

    try:
        while True:
            time.sleep(10)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        logger.info("스케줄러가 중지되었습니다.")

if __name__ == "__main__":
    start_scheduler()
