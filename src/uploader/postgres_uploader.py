"""
개선된 데이터베이스 초기화 모듈 - PostgresUploader의 연결 로직 재사용
"""
import os
import psycopg2
from dotenv import load_dotenv
from src.utils.logger import get_logger
from src.uploader.postgres_uploader import PostgresUploader

load_dotenv()
logger = get_logger(__name__)

class DatabaseInitializer:
    """데이터베이스 초기화 전용 클래스"""
    
    def __init__(self):
        # PostgresUploader의 연결 관리 기능 재사용
        self.uploader = PostgresUploader(min_connections=1, max_connections=2)
        self.logger = get_logger(__name__)
    
    def create_tables(self):
        """테이블 생성 및 스키마 업데이트"""
        if not self.uploader.is_connected():
            self.logger.error("데이터베이스에 연결할 수 없습니다. 테이블 생성을 중단합니다.")
            return False
        
        # 테이블 정의를 구조화된 형태로 관리
        tables_config = {
            'klines_1m': {
                'base_sql': """
                CREATE TABLE IF NOT EXISTS klines_1m (
                    timestamp TIMESTAMPTZ NOT NULL,
                    symbol VARCHAR(20) NOT NULL,
                    open NUMERIC,
                    high NUMERIC,
                    low NUMERIC,
                    close NUMERIC,
                    volume NUMERIC,
                    PRIMARY KEY (timestamp, symbol)
                );
                """,
                'indicators': [
                    "ema_20 NUMERIC", "rsi_14 NUMERIC", "macd NUMERIC", 
                    "macd_signal NUMERIC", "macd_hist NUMERIC", "atr NUMERIC",
                    "adx NUMERIC", "sma_50 NUMERIC", "sma_200 NUMERIC",
                    "bb_upper NUMERIC", "bb_middle NUMERIC", "bb_lower NUMERIC",
                    "stoch_k NUMERIC", "stoch_d NUMERIC",
                    # 새로운 지표들 (postgres_uploader와 일치)
                    "volume_sma_20 NUMERIC", "volume_ratio NUMERIC", 
                    "price_momentum_5m NUMERIC", "volatility_20d NUMERIC"
                ]
            },
            'funding_rates': {
                'base_sql': """
                CREATE TABLE IF NOT EXISTS funding_rates (
                    timestamp TIMESTAMPTZ PRIMARY KEY,
                    symbol VARCHAR(20) NOT NULL,
                    funding_rate NUMERIC
                );
                """
            },
            'open_interest': {
                'base_sql': """
                CREATE TABLE IF NOT EXISTS open_interest (
                    timestamp TIMESTAMPTZ PRIMARY KEY,
                    symbol VARCHAR(20) NOT NULL,
                    open_interest NUMERIC
                );
                """
            }
        }
        
        try:
            # PostgresUploader의 연결 컨텍스트 매니저 사용
            with self.uploader.get_connection() as conn:
                cur = conn.cursor()
                
                # 각 테이블 생성
                for table_name, config in tables_config.items():
                    self.logger.info(f"테이블 {table_name} 생성/업데이트 중...")
                    
                    # 기본 테이블 생성
                    cur.execute(config['base_sql'])
                    
                    # 지표 컬럼 추가 (klines_1m만)
                    if 'indicators' in config:
                        for indicator in config['indicators']:
                            try:
                                cur.execute(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {indicator};")
                            except psycopg2.Error as e:
                                # 컬럼이 이미 존재하는 경우 등 무시
                                self.logger.debug(f"컬럼 추가 스킵 ({indicator}): {e}")
                
                # 기본 키 확인 및 수정 (klines_1m)
                self._ensure_primary_key(cur)
                
                conn.commit()
                self.logger.info("모든 테이블이 성공적으로 생성/업데이트되었습니다.")
                return True
                
        except (Exception, psycopg2.Error) as error:
            self.logger.error(f"테이블 생성 중 오류 발생: {error}")
            return False
    
    def _ensure_primary_key(self, cursor):
        """klines_1m 테이블의 기본 키 확인 및 수정"""
        try:
            # 현재 기본 키 확인
            cursor.execute("""
                SELECT con.conname, array_agg(a.attname ORDER BY a.attnum)
                FROM pg_catalog.pg_constraint con
                JOIN pg_catalog.pg_attribute a ON a.attnum = ANY(con.conkey)
                WHERE con.conrelid = 'klines_1m'::regclass AND con.contype = 'p'
                GROUP BY con.conname;
            """)
            
            pk_info = cursor.fetchone()
            
            if pk_info:
                pk_name, pk_cols = pk_info
                expected_cols = ['timestamp', 'symbol']
                
                if set(pk_cols) != set(expected_cols):
                    self.logger.info(f"기본 키 수정 중: {pk_cols} -> {expected_cols}")
                    cursor.execute(f"ALTER TABLE klines_1m DROP CONSTRAINT IF EXISTS {pk_name};")
                    cursor.execute("ALTER TABLE klines_1m ADD PRIMARY KEY (timestamp, symbol);")
            else:
                # 기본 키가 없는 경우 추가
                self.logger.info("기본 키 추가 중...")
                cursor.execute("ALTER TABLE klines_1m ADD PRIMARY KEY (timestamp, symbol);")
                
        except psycopg2.Error as e:
            self.logger.warning(f"기본 키 설정 중 오류 (무시됨): {e}")
    
    def verify_schema(self):
        """스키마 검증"""
        if not self.uploader.is_connected():
            self.logger.error("데이터베이스에 연결할 수 없습니다. 스키마 검증을 중단합니다.")
            return False
        
        try:
            with self.uploader.get_connection() as conn:
                cur = conn.cursor()
                
                # 테이블 존재 확인
                cur.execute("""
                    SELECT table_name FROM information_schema.tables 
                    WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
                    AND table_name IN ('klines_1m', 'funding_rates', 'open_interest');
                """)
                
                existing_tables = {row[0] for row in cur.fetchall()}
                expected_tables = {'klines_1m', 'funding_rates', 'open_interest'}
                
                missing_tables = expected_tables - existing_tables
                if missing_tables:
                    self.logger.error(f"누락된 테이블: {missing_tables}")
                    return False
                
                # klines_1m 컬럼 확인
                cur.execute("""
                    SELECT column_name FROM information_schema.columns 
                    WHERE table_schema = 'public' 
                    AND table_name = 'klines_1m'
                    ORDER BY ordinal_position;
                """)
                
                existing_columns = {row[0] for row in cur.fetchall()}
                required_columns = {
                    'timestamp', 'symbol', 'open', 'high', 'low', 'close', 'volume',
                    'ema_20', 'rsi_14', 'macd', 'atr', 'sma_50', 'sma_200'
                }
                
                missing_columns = required_columns - existing_columns
                if missing_columns:
                    self.logger.warning(f"누락된 컬럼 (klines_1m): {missing_columns}")
                
                self.logger.info("스키마 검증 완료")
                return True
                
        except (Exception, psycopg2.Error) as error:
            self.logger.error(f"스키마 검증 중 오류 발생: {error}")
            return False
    
    def create_indexes(self):
        """성능 최적화를 위한 인덱스 생성"""
        if not self.uploader.is_connected():
            self.logger.error("데이터베이스에 연결할 수 없습니다. 인덱스 생성을 중단합니다.")
            return False
        
        try:
            with self.uploader.get_connection() as conn:
                cur = conn.cursor()
                
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
                        self.logger.info(f"인덱스 생성: {query.split('idx_')[1].split(' ')[0]}")
                    except psycopg2.Error as e:
                        self.logger.debug(f"인덱스 생성 스킵 (이미 존재): {e}")
                        conn.rollback()
                
                return True
                
        except (Exception, psycopg2.Error) as error:
            self.logger.error(f"인덱스 생성 중 오류 발생: {error}")
            return False
    
    def get_table_stats(self):
        """테이블 통계 조회"""
        if not self.uploader.is_connected():
            return None
        
        try:
            with self.uploader.get_connection() as conn:
                cur = conn.cursor()
                
                stats = {}
                tables = ['klines_1m', 'funding_rates', 'open_interest']
                
                for table in tables:
                    cur.execute(f"SELECT COUNT(*) FROM {table}")
                    count = cur.fetchone()[0]
                    stats[table] = count
                
                return stats
                
        except (Exception, psycopg2.Error) as error:
            self.logger.error(f"테이블 통계 조회 중 오류 발생: {error}")
            return None
    
    def close(self):
        """연결 정리"""
        if self.uploader:
            self.uploader.close()

# 편의 함수들 (기존 호환성 유지)
def get_db_connection():
    """Deprecated: PostgresUploader 사용을 권장"""
    import warnings
    warnings.warn("get_db_connection is deprecated. Use DatabaseInitializer instead.", 
                  DeprecationWarning, stacklevel=2)
    
    uploader = PostgresUploader(min_connections=1, max_connections=1)
    return uploader.get_connection()

def create_tables():
    """테이블 생성 (기존 호환성 유지)"""
    initializer = DatabaseInitializer()
    try:
        success = initializer.create_tables()
        if success:
            initializer.create_indexes()  # 인덱스도 함께 생성
        return success
    finally:
        initializer.close()

def verify_database_setup():
    """데이터베이스 설정 검증"""
    initializer = DatabaseInitializer()
    try:
        return initializer.verify_schema()
    finally:
        initializer.close()

if __name__ == '__main__':
    print("데이터베이스 초기화 중...")
    
    initializer = DatabaseInitializer()
    try:
        # 테이블 생성
        if initializer.create_tables():
            print("✅ 테이블 생성 완료")
            
            # 인덱스 생성
            if initializer.create_indexes():
                print("✅ 인덱스 생성 완료")
            
            # 스키마 검증
            if initializer.verify_schema():
                print("✅ 스키마 검증 완료")
                
                # 테이블 통계
                stats = initializer.get_table_stats()
                if stats:
                    print("📊 테이블 통계:")
                    for table, count in stats.items():
                        print(f"  - {table}: {count:,}개 레코드")
                
                print("🎉 데이터베이스 초기화 성공!")
            else:
                print("❌ 스키마 검증 실패")
        else:
            print("❌ 테이블 생성 실패")
            
    except Exception as e:
        print(f"❌ 초기화 실패: {e}")
    finally:
        initializer.close()