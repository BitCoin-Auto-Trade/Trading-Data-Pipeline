"""
최적화된 PostgreSQL 업로더 - 배치 처리, 연결 풀, 성능 개선
"""
import psycopg2
import psycopg2.extras
import pandas as pd
import os
from contextlib import contextmanager
from typing import List, Dict
from src.utils.logger import get_logger
from psycopg2 import pool

class OptimizedPostgresUploader:
    """성능 최적화된 PostgreSQL 업로더"""
    
    def __init__(self, min_connections=1, max_connections=5):
        self.logger = get_logger(__name__)
        self.connection_pool = None
        self.min_connections = min_connections
        self.max_connections = max_connections
        
        # 성능 메트릭
        self.upload_count = 0
        self.batch_upload_count = 0
        self.connection_errors = 0
        
        self._initialize_connection_pool()
    
    def _initialize_connection_pool(self):
        """연결 풀 초기화"""
        try:
            self.connection_pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=self.min_connections,
                maxconn=self.max_connections,
                host=os.getenv("POSTGRES_HOST", "localhost"),
                database=os.getenv("POSTGRES_DB", "postgres"),
                user=os.getenv("POSTGRES_USER", "postgres"),
                password=os.getenv("POSTGRES_PASSWORD", ""),
                port=int(os.getenv("POSTGRES_PORT", 5432)),
                # 연결 유지 설정
                keepalives_idle=600,  # 10분
                keepalives_interval=30,
                keepalives_count=3
            )
            self.logger.info(f"PostgreSQL 연결 풀이 성공적으로 생성되었습니다 ({self.min_connections}-{self.max_connections})")
            
        except psycopg2.Error as e:
            self.logger.error(f"연결 풀 생성 실패: {e}")
            self.connection_pool = None
    
    @contextmanager
    def get_connection(self):
        """연결 풀에서 안전한 연결 가져오기"""
        conn = None
        try:
            if not self.connection_pool:
                self._initialize_connection_pool()
                
            conn = self.connection_pool.getconn()
            if conn:
                # 연결 상태 확인
                conn.rollback()  # 이전 트랜잭션 정리
                yield conn
            else:
                raise psycopg2.Error("연결 풀에서 연결을 가져올 수 없습니다")
                
        except psycopg2.Error as e:
            self.connection_errors += 1
            self.logger.error(f"DB 연결 오류: {e}")
            if conn:
                conn.rollback()
            raise
        finally:
            if conn and self.connection_pool:
                self.connection_pool.putconn(conn)
    
    def is_connected(self) -> bool:
        """연결 상태 확인"""
        if not self.connection_pool:
            return False
        
        try:
            with self.get_connection() as conn:
                cur = conn.cursor()
                cur.execute("SELECT 1")
                return True
        except:
            return False
    
    def get_historical_klines(self, symbol: str, limit: int = 100) -> pd.DataFrame:
        """최적화된 과거 kline 데이터 조회"""
        if not self.is_connected():
            self.logger.error("과거 kline을 가져올 수 없습니다. PostgreSQL에 연결되어 있지 않습니다.")
            return pd.DataFrame()

        # 인덱스를 활용한 최적화된 쿼리
        sql = """
        SELECT timestamp, symbol, open, high, low, close, volume,
               ema_20, rsi_14, macd, macd_signal, macd_hist, atr, adx,
               sma_50, sma_200, bb_upper, bb_middle, bb_lower, stoch_k, stoch_d,
               volume_sma_20, volume_ratio, price_momentum_5m, volatility_20d
        FROM klines_1m 
        WHERE symbol = %s 
        ORDER BY timestamp DESC 
        LIMIT %s
        """
        
        try:
            with self.get_connection() as conn:
                df = pd.read_sql(sql, conn, params=(symbol, limit))
                
                if not df.empty:
                    df.set_index('timestamp', inplace=True)
                    df.sort_index(inplace=True)
                
                self.logger.info(f"데이터베이스에서 {symbol}에 대한 {len(df)}개의 과거 kline을 가져왔습니다.")
                return df
                
        except (Exception, psycopg2.Error) as error:
            self.logger.error(f"과거 kline 데이터를 가져오는 중 오류 발생: {error}")
            return pd.DataFrame()
    
    def upload_kline_data(self, kline_data: dict):
        """개별 kline 데이터 업로드 (기존 호환성 유지)"""
        if not self.is_connected():
            self.logger.error("kline 데이터를 업로드할 수 없습니다. PostgreSQL에 연결되어 있지 않습니다.")
            return

        sql = """
        INSERT INTO klines_1m (
            timestamp, symbol, open, high, low, close, volume,
            ema_20, rsi_14, macd, macd_signal, macd_hist, atr, adx,
            sma_50, sma_200, bb_upper, bb_middle, bb_lower, stoch_k, stoch_d,
            volume_sma_20, volume_ratio, price_momentum_5m, volatility_20d
        ) VALUES (
            %(timestamp)s, %(symbol)s, %(open)s, %(high)s, %(low)s, %(close)s, %(volume)s,
            %(ema_20)s, %(rsi_14)s, %(macd)s, %(macd_signal)s, %(macd_hist)s, %(atr)s, %(adx)s,
            %(sma_50)s, %(sma_200)s, %(bb_upper)s, %(bb_middle)s, %(bb_lower)s, %(stoch_k)s, %(stoch_d)s,
            %(volume_sma_20)s, %(volume_ratio)s, %(price_momentum_5m)s, %(volatility_20d)s
        )
        ON CONFLICT (timestamp, symbol) DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume,
            ema_20 = EXCLUDED.ema_20,
            rsi_14 = EXCLUDED.rsi_14,
            macd = EXCLUDED.macd,
            macd_signal = EXCLUDED.macd_signal,
            macd_hist = EXCLUDED.macd_hist,
            atr = EXCLUDED.atr,
            adx = EXCLUDED.adx,
            sma_50 = EXCLUDED.sma_50,
            sma_200 = EXCLUDED.sma_200,
            bb_upper = EXCLUDED.bb_upper,
            bb_middle = EXCLUDED.bb_middle,
            bb_lower = EXCLUDED.bb_lower,
            stoch_k = EXCLUDED.stoch_k,
            stoch_d = EXCLUDED.stoch_d,
            volume_sma_20 = EXCLUDED.volume_sma_20,
            volume_ratio = EXCLUDED.volume_ratio,
            price_momentum_5m = EXCLUDED.price_momentum_5m,
            volatility_20d = EXCLUDED.volatility_20d
        """
        
        try:
            with self.get_connection() as conn:
                cur = conn.cursor()
                cur.execute(sql, kline_data)
                conn.commit()
                self.upload_count += 1
                self.logger.debug(f"{kline_data['timestamp']}에 {kline_data['symbol']}에 대한 kline 데이터를 업로드했습니다.")
                
        except (Exception, psycopg2.Error) as error:
            self.logger.error(f"kline 데이터 업로드 중 오류 발생: {error}")
    
    def batch_upload_klines(self, kline_data_list: List[Dict]) -> int:
        """배치 kline 데이터 업로드 - 대폭 성능 개선"""
        if not kline_data_list:
            return 0
            
        if not self.is_connected():
            self.logger.error("배치 kline 데이터를 업로드할 수 없습니다. PostgreSQL에 연결되어 있지 않습니다.")
            return 0

        sql = """
        INSERT INTO klines_1m (
            timestamp, symbol, open, high, low, close, volume,
            ema_20, rsi_14, macd, macd_signal, macd_hist, atr, adx,
            sma_50, sma_200, bb_upper, bb_middle, bb_lower, stoch_k, stoch_d,
            volume_sma_20, volume_ratio, price_momentum_5m, volatility_20d
        ) VALUES (
            %(timestamp)s, %(symbol)s, %(open)s, %(high)s, %(low)s, %(close)s, %(volume)s,
            %(ema_20)s, %(rsi_14)s, %(macd)s, %(macd_signal)s, %(macd_hist)s, %(atr)s, %(adx)s,
            %(sma_50)s, %(sma_200)s, %(bb_upper)s, %(bb_middle)s, %(bb_lower)s, %(stoch_k)s, %(stoch_d)s,
            %(volume_sma_20)s, %(volume_ratio)s, %(price_momentum_5m)s, %(volatility_20d)s
        )
        ON CONFLICT (timestamp, symbol) DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume,
            ema_20 = EXCLUDED.ema_20,
            rsi_14 = EXCLUDED.rsi_14,
            macd = EXCLUDED.macd,
            macd_signal = EXCLUDED.macd_signal,
            macd_hist = EXCLUDED.macd_hist,
            atr = EXCLUDED.atr,
            adx = EXCLUDED.adx,
            sma_50 = EXCLUDED.sma_50,
            sma_200 = EXCLUDED.sma_200,
            bb_upper = EXCLUDED.bb_upper,
            bb_middle = EXCLUDED.bb_middle,
            bb_lower = EXCLUDED.bb_lower,
            stoch_k = EXCLUDED.stoch_k,
            stoch_d = EXCLUDED.stoch_d,
            volume_sma_20 = EXCLUDED.volume_sma_20,
            volume_ratio = EXCLUDED.volume_ratio,
            price_momentum_5m = EXCLUDED.price_momentum_5m,
            volatility_20d = EXCLUDED.volatility_20d
        """
        
        try:
            with self.get_connection() as conn:
                cur = conn.cursor()
                
                # 배치 실행으로 성능 대폭 개선
                psycopg2.extras.execute_batch(
                    cur, sql, kline_data_list, page_size=100
                )
                
                conn.commit()
                self.batch_upload_count += 1
                upload_count = len(kline_data_list)
                self.upload_count += upload_count
                
                self.logger.info(f"{upload_count}개의 kline 데이터를 배치 업로드했습니다.")
                return upload_count
                
        except (Exception, psycopg2.Error) as error:
            self.logger.error(f"배치 kline 데이터 업로드 중 오류 발생: {error}")
            return 0
    
    def bulk_upload_klines_copy(self, kline_data_list: List[Dict]) -> int:
        """COPY를 사용한 초고속 대량 업로드 (가장 빠름)"""
        if not kline_data_list:
            return 0
            
        if not self.is_connected():
            self.logger.error("대량 kline 데이터를 업로드할 수 없습니다. PostgreSQL에 연결되어 있지 않습니다.")
            return 0

        try:
            import io
            import csv
            
            # CSV 형태로 데이터 준비
            output = io.StringIO()
            writer = csv.writer(output, delimiter='\t')
            
            for item in kline_data_list:
                writer.writerow([
                    item['timestamp'], item['symbol'], item['open'], 
                    item['high'], item['low'], item['close'], item['volume'],
                    item.get('ema_20'), item.get('rsi_14'), item.get('macd'),
                    item.get('macd_signal'), item.get('macd_hist'), item.get('atr'),
                    item.get('adx'), item.get('sma_50'), item.get('sma_200'),
                    item.get('bb_upper'), item.get('bb_middle'), item.get('bb_lower'),
                    item.get('stoch_k'), item.get('stoch_d'), item.get('volume_sma_20'),
                    item.get('volume_ratio'), item.get('price_momentum_5m'), item.get('volatility_20d')
                ])
            
            output.seek(0)
            
            with self.get_connection() as conn:
                cur = conn.cursor()
                
                # 임시 테이블에 COPY
                cur.execute("""
                    CREATE TEMP TABLE temp_klines (LIKE klines_1m INCLUDING ALL)
                """)
                
                # COPY로 초고속 삽입
                cur.copy_expert("""
                    COPY temp_klines (
                        timestamp, symbol, open, high, low, close, volume,
                        ema_20, rsi_14, macd, macd_signal, macd_hist, atr, adx,
                        sma_50, sma_200, bb_upper, bb_middle, bb_lower, stoch_k, stoch_d,
                        volume_sma_20, volume_ratio, price_momentum_5m, volatility_20d
                    ) FROM STDIN WITH (FORMAT CSV, DELIMITER E'\t', NULL '')
                """, output)
                
                # UPSERT 실행
                cur.execute("""
                    INSERT INTO klines_1m 
                    SELECT * FROM temp_klines
                    ON CONFLICT (timestamp, symbol) DO UPDATE SET
                        open = EXCLUDED.open,
                        high = EXCLUDED.high,
                        low = EXCLUDED.low,
                        close = EXCLUDED.close,
                        volume = EXCLUDED.volume,
                        ema_20 = EXCLUDED.ema_20,
                        rsi_14 = EXCLUDED.rsi_14,
                        macd = EXCLUDED.macd,
                        macd_signal = EXCLUDED.macd_signal,
                        macd_hist = EXCLUDED.macd_hist,
                        atr = EXCLUDED.atr,
                        adx = EXCLUDED.adx,
                        sma_50 = EXCLUDED.sma_50,
                        sma_200 = EXCLUDED.sma_200,
                        bb_upper = EXCLUDED.bb_upper,
                        bb_middle = EXCLUDED.bb_middle,
                        bb_lower = EXCLUDED.bb_lower,
                        stoch_k = EXCLUDED.stoch_k,
                        stoch_d = EXCLUDED.stoch_d,
                        volume_sma_20 = EXCLUDED.volume_sma_20,
                        volume_ratio = EXCLUDED.volume_ratio,
                        price_momentum_5m = EXCLUDED.price_momentum_5m,
                        volatility_20d = EXCLUDED.volatility_20d
                """)
                
                conn.commit()
                upload_count = len(kline_data_list)
                self.upload_count += upload_count
                
                self.logger.info(f"{upload_count}개의 kline 데이터를 COPY로 대량 업로드했습니다.")
                return upload_count
                
        except (Exception, psycopg2.Error) as error:
            self.logger.error(f"COPY 대량 업로드 중 오류 발생: {error}")
            return 0
    
    def upload_funding_rate(self, funding_rate_data: dict):
        """펀딩 비율 데이터 업로드"""
        if not self.is_connected():
            self.logger.error("펀딩 비율을 업로드할 수 없습니다. PostgreSQL에 연결되어 있지 않습니다.")
            return

        sql = """
        INSERT INTO funding_rates (timestamp, symbol, funding_rate)
        VALUES (%(timestamp)s, %(symbol)s, %(funding_rate)s)
        ON CONFLICT (timestamp) DO UPDATE SET
            funding_rate = EXCLUDED.funding_rate
        """
        
        try:
            with self.get_connection() as conn:
                cur = conn.cursor()
                cur.execute(sql, funding_rate_data)
                conn.commit()
                self.logger.debug(f"{funding_rate_data['timestamp']}에 {funding_rate_data['symbol']}에 대한 펀딩 비율을 업로드했습니다.")
                
        except (Exception, psycopg2.Error) as error:
            self.logger.error(f"펀딩 비율 업로드 중 오류 발생: {error}")

    def upload_open_interest(self, open_interest_data: dict):
        """미결제 약정 데이터 업로드"""
        if not self.is_connected():
            self.logger.error("미결제 약정을 업로드할 수 없습니다. PostgreSQL에 연결되어 있지 않습니다.")
            return

        sql = """
        INSERT INTO open_interest (timestamp, symbol, open_interest)
        VALUES (%(timestamp)s, %(symbol)s, %(open_interest)s)
        ON CONFLICT (timestamp) DO UPDATE SET
            open_interest = EXCLUDED.open_interest
        """
        
        try:
            with self.get_connection() as conn:
                cur = conn.cursor()
                cur.execute(sql, open_interest_data)
                conn.commit()
                self.logger.debug(f"{open_interest_data['timestamp']}에 {open_interest_data['symbol']}에 대한 미결제 약정을 업로드했습니다.")
                
        except (Exception, psycopg2.Error) as error:
            self.logger.error(f"미결제 약정 업로드 중 오류 발생: {error}")
    
    def get_performance_stats(self) -> dict:
        """성능 통계 조회"""
        return {
            "total_uploads": self.upload_count,
            "batch_uploads": self.batch_upload_count,
            "connection_errors": self.connection_errors,
            "is_connected": self.is_connected(),
            "pool_stats": {
                "min_connections": self.min_connections,
                "max_connections": self.max_connections
            }
        }
    
    def optimize_database(self):
        """데이터베이스 최적화 실행"""
        if not self.is_connected():
            self.logger.error("데이터베이스 최적화를 실행할 수 없습니다.")
            return
        
        try:
            with self.get_connection() as conn:
                cur = conn.cursor()
                
                # 인덱스 생성 (없는 경우에만)
                index_queries = [
                    """CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_klines_symbol_timestamp 
                       ON klines_1m (symbol, timestamp DESC)""",
                    """CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_klines_timestamp 
                       ON klines_1m (timestamp DESC)""",
                    """CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_funding_rates_timestamp 
                       ON funding_rates (timestamp DESC)""",
                    """CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_open_interest_timestamp 
                       ON open_interest (timestamp DESC)"""
                ]
                
                for query in index_queries:
                    try:
                        cur.execute(query)
                        conn.commit()
                    except psycopg2.Error as e:
                        self.logger.warning(f"인덱스 생성 실패 (이미 존재할 수 있음): {e}")
                        conn.rollback()
                
                # 통계 업데이트
                cur.execute("ANALYZE klines_1m")
                cur.execute("ANALYZE funding_rates")
                cur.execute("ANALYZE open_interest")
                conn.commit()
                
                self.logger.info("데이터베이스 최적화가 완료되었습니다.")
                
        except (Exception, psycopg2.Error) as error:
            self.logger.error(f"데이터베이스 최적화 중 오류 발생: {error}")
    
    def close(self):
        """연결 풀 종료"""
        if self.connection_pool:
            self.connection_pool.closeall()
            self.logger.info("PostgreSQL 연결 풀이 종료되었습니다.")

# 기존 클래스명 호환성 유지
PostgresUploader = OptimizedPostgresUploader

# 사용 예제
if __name__ == '__main__':
    uploader = OptimizedPostgresUploader()
    
    if uploader.is_connected():
        # 성능 통계 확인
        stats = uploader.get_performance_stats()
        print("성능 통계:", stats)
        
        # 데이터베이스 최적화 실행
        uploader.optimize_database()
        
        # 과거 데이터 테스트
        historical_data = uploader.get_historical_klines('BTCUSDT', limit=5)
        print("과거 데이터 샘플:")
        print(historical_data.head())
        
        uploader.close()
    else:
        print("PostgreSQL에 연결하지 못했습니다. .env 및 데이터베이스 상태를 확인하십시오.")