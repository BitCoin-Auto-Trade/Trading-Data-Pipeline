"""
개선된 스케줄러 - 겹침 방지, 성능 모니터링, 안정성 강화
"""
import sys
import os

# 프로젝트 루트 디렉토리를 Python 경로에 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from apscheduler.schedulers.background import BackgroundScheduler
import time
import datetime
import pandas as pd
import threading
from src.collector.binance_client import BinanceClient
from src.formatter.indicator_calculator import IndicatorCalculator
from src.uploader.postgres_uploader import PostgresUploader
from src.utils.logger import get_logger

logger = get_logger("scheduler")

class TradingDataScheduler:
    """개선된 스케줄러 클래스"""
    
    def __init__(self):
        self.collector = BinanceClient()
        self.formatter = IndicatorCalculator()
        self.uploader = PostgresUploader()
        self.symbol = "BTCUSDT"
        
        # 겹침 방지용 플래그
        self.is_processing = False
        self.processing_lock = threading.Lock()
        
        # 성능 모니터링
        self.processing_times = []
        self.success_count = 0
        self.error_count = 0
        self.last_processing_time = None
        
        # 설정
        self.max_processing_time = 50  # 50초 이상 걸리면 경고
        self.max_consecutive_errors = 3
        self.consecutive_errors = 0
        
    def is_uploader_ready(self) -> bool:
        """업로더 상태 확인"""
        if not self.uploader.is_connected():
            logger.error("PostgreSQL 업로더가 연결되지 않았습니다.")
            return False
        return True
    
    def run_data_pipeline(self):
        """
        겹침 방지 로직이 있는 안전한 데이터 처리
        """
        # 겹침 방지 체크
        if self.is_processing:
            logger.warning("이전 처리가 아직 진행 중입니다. 이번 실행을 건너뜁니다.")
            return
        
        # 연속 오류 체크
        if self.consecutive_errors >= self.max_consecutive_errors:
            logger.error(f"연속 {self.consecutive_errors}회 오류 발생. 스케줄러를 일시 중단합니다.")
            return
        
        with self.processing_lock:
            self.is_processing = True
            start_time = time.time()
            
            try:
                logger.info("=== 바이낸스 데이터 처리 시작 ===")
                
                # 업로더 상태 확인
                if not self.is_uploader_ready():
                    return
                
                # 단계별 처리
                success = self._process_kline_data()
                
                if success:
                    self._process_funding_and_oi_data()
                
                # 성공 처리
                processing_time = time.time() - start_time
                self._log_success(processing_time)
                
            except Exception as e:
                # 오류 처리
                processing_time = time.time() - start_time
                self._log_error(e, processing_time)
                
            finally:
                self.is_processing = False
                self.last_processing_time = datetime.datetime.now()
    
    def _process_kline_data(self) -> bool:
        """K-line 데이터 처리 (메인 로직)"""
        try:
            # 1. 과거 데이터 조회 (기존 200개 → 50개로 최적화)
            historical_klines_df = self.uploader.get_historical_klines(self.symbol, limit=50)
            
            # 2. 신규 데이터 시작 시간 결정
            start_time, end_time = self._determine_data_range(historical_klines_df)
            
            if start_time >= end_time:
                logger.info("수집할 새로운 데이터가 없습니다.")
                return True
            
            # 3. 신규 데이터 수집
            logger.info(f"신규 데이터 수집: {start_time} ~ {end_time}")
            new_raw_klines = self.collector.get_historical_klines(
                self.symbol, "1m", start_time, end_time
            )
            
            if not new_raw_klines:
                logger.info("바이낸스에서 새로운 kline 데이터를 가져오지 못했습니다.")
                return True
            
            # 4. 데이터 포맷팅 및 지표 계산
            success = self._format_and_calculate_indicators(
                new_raw_klines, historical_klines_df
            )
            
            return success
            
        except Exception as e:
            logger.error(f"K-line 데이터 처리 중 오류: {e}", exc_info=True)
            return False
    
    def _determine_data_range(self, historical_df: pd.DataFrame) -> tuple:
        """수집할 데이터 범위 결정"""
        if not historical_df.empty:
            last_timestamp = historical_df.index.max().to_pydatetime()
            start_time = (last_timestamp + datetime.timedelta(minutes=1)).strftime('%Y-%m-%d %H:%M:%S')
        else:
            # 초기 실행 시 마지막 50분 데이터 수집
            start_time = (datetime.datetime.now() - datetime.timedelta(minutes=50)).strftime('%Y-%m-%d %H:%M:%S')
        
        # 현재 분 시작 직전까지만 수집
        end_time = (
            datetime.datetime.now()
            .replace(second=0, microsecond=0) 
            - datetime.timedelta(seconds=1)
        ).strftime('%Y-%m-%d %H:%M:%S')
        
        return start_time, end_time
    
    def _format_and_calculate_indicators(self, new_raw_klines: list, 
                                       historical_df: pd.DataFrame) -> bool:
        """데이터 포맷팅 및 지표 계산"""
        try:
            # 1. 신규 데이터 포맷팅
            new_klines_df = self.formatter.format_klines(new_raw_klines)
            if new_klines_df is None or new_klines_df.empty:
                logger.warning("포맷팅 후 새로운 데이터가 비어있습니다.")
                return False
            
            # 2. 과거 + 신규 데이터 결합 (최소한만)
            if not historical_df.empty:
                # 지표 계산에 필요한 최소 데이터만 결합
                required_history = 30  # 가장 긴 지표 기간
                recent_history = historical_df.tail(required_history)
                combined_df = pd.concat([recent_history, new_klines_df])
            else:
                combined_df = new_klines_df
            
            # 중복 제거 및 정렬
            combined_df = combined_df[~combined_df.index.duplicated(keep='last')]
            combined_df = combined_df.sort_index()
            
            # 3. 지표 계산 (전체 데이터에 대해)
            combined_df = self._calculate_all_indicators(combined_df)
            
            # 4. 업로드할 신규 데이터만 필터링
            rows_to_upload = combined_df[combined_df.index.isin(new_klines_df.index)]
            
            # 5. 데이터베이스 업로드
            upload_count = self._upload_kline_data(rows_to_upload)
            logger.info(f"{upload_count}개의 새로운 kline 레코드를 업로드했습니다.")
            
            return True
            
        except Exception as e:
            logger.error(f"지표 계산 및 업로드 중 오류: {e}", exc_info=True)
            return False
    
    def _calculate_all_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """모든 기술적 지표 계산"""
        try:
            logger.debug("기술적 지표 계산 시작...")
            
            # 기존 지표들
            df = self.formatter.calculate_ema(df, period=20)
            df = self.formatter.calculate_rsi(df, period=14)
            df = self.formatter.calculate_macd(df)
            df = self.formatter.calculate_atr(df, period=14)
            df = self.formatter.calculate_adx(df, period=14)
            df = self.formatter.calculate_sma(df, period=50)
            df = self.formatter.calculate_sma(df, period=200)
            df = self.formatter.calculate_bollinger_bands(df, period=20)
            df = self.formatter.calculate_stochastic_oscillator(df, k_period=14, d_period=3)
            df = self.formatter.calculate_volume_sma(df, period=20)
            df = self.formatter.calculate_volume_ratio(df, period=20)
            df = self.formatter.calculate_price_momentum(df, period=5)
            df = self.formatter.calculate_volatility(df, period=20)
            
            logger.debug("기술적 지표 계산 완료")
            return df
            
        except Exception as e:
            logger.error(f"지표 계산 중 오류: {e}", exc_info=True)
            raise
    
    def _upload_kline_data(self, df: pd.DataFrame) -> int:
        """K-line 데이터 업로드"""
        upload_count = 0
        
        for index, row in df.iterrows():
            try:
                kline_data = {
                    'timestamp': index.to_pydatetime(),
                    'symbol': self.symbol,
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
                    'atr': row.get('atr_14'),
                    'adx': row.get('adx'),
                    'sma_50': row.get('sma_50'),
                    'sma_200': row.get('sma_200'),
                    'bb_upper': row.get('bb_upper'),
                    'bb_middle': row.get('bb_middle'),
                    'bb_lower': row.get('bb_lower'),
                    'stoch_k': row.get('stoch_k'),
                    'stoch_d': row.get('stoch_d'),
                    'volume_sma_20': row.get('volume_sma_20'),
                    'volume_ratio': row.get('volume_ratio'),
                    'price_momentum_5m': row.get('price_momentum_5m'),
                    'volatility_20d': row.get('volatility_20d')
                }
                
                self.uploader.upload_kline_data(kline_data)
                upload_count += 1
                
            except Exception as e:
                logger.error(f"개별 kline 데이터 업로드 실패 ({index}): {e}")
        
        return upload_count
    
    def _process_funding_and_oi_data(self):
        """펀딩비 및 미결제약정 데이터 처리"""
        try:
            # 펀딩비 처리
            raw_funding_rate = self.collector.get_funding_rate(self.symbol, limit=1)
            if raw_funding_rate and len(raw_funding_rate) > 0:
                latest_funding_rate = raw_funding_rate[0]
                funding_rate_data = {
                    'timestamp': datetime.datetime.fromtimestamp(latest_funding_rate['fundingTime'] / 1000),
                    'symbol': latest_funding_rate['symbol'],
                    'funding_rate': float(latest_funding_rate['fundingRate'])
                }
                self.uploader.upload_funding_rate(funding_rate_data)
                logger.debug("펀딩비 데이터 업로드 완료")
            
            # 미결제약정 처리
            raw_open_interest = self.collector.get_open_interest(self.symbol)
            if raw_open_interest:
                open_interest_data = {
                    'timestamp': datetime.datetime.fromtimestamp(raw_open_interest['time'] / 1000),
                    'symbol': raw_open_interest['symbol'],
                    'open_interest': float(raw_open_interest['openInterest'])
                }
                self.uploader.upload_open_interest(open_interest_data)
                logger.debug("미결제약정 데이터 업로드 완료")
                
        except Exception as e:
            logger.error(f"펀딩비/미결제약정 처리 중 오류: {e}", exc_info=True)
    
    def _log_success(self, processing_time: float):
        """성공 로그 및 성능 모니터링"""
        self.success_count += 1
        self.consecutive_errors = 0  # 연속 오류 카운터 리셋
        self.processing_times.append(processing_time)
        
        # 최근 10개 평균 계산
        if len(self.processing_times) > 10:
            self.processing_times = self.processing_times[-10:]
        
        avg_time = sum(self.processing_times) / len(self.processing_times)
        
        logger.info(
            f"=== 처리 완료 === "
            f"시간: {processing_time:.2f}초 | "
            f"평균: {avg_time:.2f}초 | "
            f"성공: {self.success_count}회 | "
            f"오류: {self.error_count}회"
        )
        
        # 성능 경고
        if processing_time > self.max_processing_time:
            logger.warning(f"처리 시간이 임계값({self.max_processing_time}초)을 초과했습니다: {processing_time:.2f}초")
    
    def _log_error(self, error: Exception, processing_time: float):
        """오류 로그 및 카운터 업데이트"""
        self.error_count += 1
        self.consecutive_errors += 1
        
        logger.error(
            f"=== 처리 실패 === "
            f"오류: {str(error)} | "
            f"처리시간: {processing_time:.2f}초 | "
            f"연속오류: {self.consecutive_errors}회 | "
            f"총오류: {self.error_count}회",
            exc_info=True
        )
    
    def get_status(self) -> dict:
        """스케줄러 상태 조회"""
        avg_time = (
            sum(self.processing_times) / len(self.processing_times) 
            if self.processing_times else 0
        )
        
        return {
            "is_processing": self.is_processing,
            "success_count": self.success_count,
            "error_count": self.error_count,
            "consecutive_errors": self.consecutive_errors,
            "average_processing_time": round(avg_time, 2),
            "last_processing_time": self.last_processing_time,
            "uploader_connected": self.uploader.is_connected()
        }


data_scheduler = TradingDataScheduler()

def start_scheduler():
    """개선된 스케줄러 시작"""
    scheduler = BackgroundScheduler()
    
    # 겹침 방지가 적용된 작업 등록
    scheduler.add_job(
        data_scheduler.run_data_pipeline, 
        'cron', 
        minute='*', 
        second=5,  # 5초에 실행
        id="binance_data_processing",
        max_instances=1  # 동시 실행 방지
    )
    
    scheduler.start()
    logger.info("스케줄러가 시작되었습니다. 매 분 5초에 데이터 처리가 실행됩니다.")
    
    return scheduler

def main():
    """메인 함수: 스케줄러를 시작하고 실행합니다."""
    scheduler = start_scheduler()
    
    try:
        # 메인 스레드가 종료되지 않도록 유지하여 백그라운드 스케줄러가 계속 실행되게 합니다.
        while True:
            time.sleep(10)
            
            # 선택 사항: 10초마다 스케줄러 상태를 확인합니다.
            status = data_scheduler.get_status()
            if status["consecutive_errors"] >= 3:
                logger.critical("연속 오류가 3회를 초과했습니다. 스케줄러를 중단합니다.")
                break
                
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        logger.info("스케줄러가 중지되었습니다.")

if __name__ == "__main__":
    main()