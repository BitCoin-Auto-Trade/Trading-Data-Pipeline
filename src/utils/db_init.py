import os
import psycopg2
from contextlib import contextmanager
from dotenv import load_dotenv
from src.utils.logger import get_logger

load_dotenv()
logger = get_logger(__name__)

@contextmanager
def get_db_connection():
    """데이터베이스 연결 컨텍스트 매니저"""
    conn = None
    try:
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST"),
            database=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            port=os.getenv("POSTGRES_PORT")
        )
        yield conn
        conn.commit()
    except psycopg2.Error as e:
        if conn:
            conn.rollback()
        logger.error(f"데이터베이스 오류: {e}")
        raise
    finally:
        if conn:
            conn.close()

def create_tables():
    """테이블 생성 및 스키마 업데이트"""
    with get_db_connection() as conn:
        cur = conn.cursor()
        
        # 테이블 정의를 딕셔너리로 관리
        tables = {
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
                    "stoch_k NUMERIC", "stoch_d NUMERIC"
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
        
        # 테이블 생성
        for table_name, config in tables.items():
            cur.execute(config['base_sql'])
            
            # 지표 컬럼 추가 (klines_1m만)
            if 'indicators' in config:
                for indicator in config['indicators']:
                    cur.execute(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {indicator};")
        
        logger.info("모든 테이블이 성공적으로 생성/업데이트되었습니다.")

def verify_schema():
    """스키마 검증"""
    with get_db_connection() as conn:
        cur = conn.cursor()
        
        # 테이블 존재 확인
        cur.execute("""
            SELECT table_name FROM information_schema.tables 
            WHERE table_schema = 'public' AND table_type = 'BASE TABLE';
        """)
        
        tables = [row[0] for row in cur.fetchall()]
        expected_tables = ['klines_1m', 'funding_rates', 'open_interest']
        
        missing_tables = set(expected_tables) - set(tables)
        if missing_tables:
            logger.warning(f"누락된 테이블: {missing_tables}")
            return False
        
        logger.info("스키마 검증 완료")
        return True

if __name__ == '__main__':
    print("데이터베이스 초기화 중...")
    try:
        create_tables()
        if verify_schema():
            print("데이터베이스 초기화 성공!")
        else:
            print("스키마 검증 실패")
    except Exception as e:
        print(f"초기화 실패: {e}")