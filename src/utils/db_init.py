import os
import psycopg2
from dotenv import load_dotenv
from src.utils.logger import get_logger

load_dotenv()
logger = get_logger(__name__)

def get_db_connection():
    """PostgreSQL 데이터베이스에 대한 연결을 설정합니다."""
    try:
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            database=os.getenv("POSTGRES_DB", "postgres"),
            user=os.getenv("POSTGRES_USER", "postgres"),
            password=os.getenv("POSTGRES_PASSWORD", ""),
            port=os.getenv("POSTGRES_PORT", 5432)
        )
        logger.info("데이터베이스 연결이 성공적으로 설정되었습니다.")
        return conn
    except psycopg2.OperationalError as e:
        logger.error(f"데이터베이스에 연결할 수 없습니다: {e}")
        return None

def create_tables():
    """PostgreSQL 데이터베이스에 테이블을 생성하고 업데이트합니다."""
    conn = None
    try:
        conn = get_db_connection()
        if conn is None:
            return
            
        cur = conn.cursor()

        # 기본 테이블 생성
        cur.execute("""
        CREATE TABLE IF NOT EXISTS klines_1m (
            timestamp TIMESTAMPTZ NOT NULL,
            symbol VARCHAR(20) NOT NULL,
            open NUMERIC,
            high NUMERIC,
            low NUMERIC,
            close NUMERIC,
            volume NUMERIC
        );
        """)

        # 지표를 위한 열 추가
        indicator_columns = [
            "ema_20 NUMERIC", "rsi_14 NUMERIC", "macd NUMERIC", "macd_signal NUMERIC",
            "macd_hist NUMERIC", "atr NUMERIC", "adx NUMERIC", "sma_50 NUMERIC",
            "sma_200 NUMERIC", "bb_upper NUMERIC", "bb_middle NUMERIC", "bb_lower NUMERIC",
            "stoch_k NUMERIC", "stoch_d NUMERIC"
        ]
        for col in indicator_columns:
            cur.execute(f"ALTER TABLE klines_1m ADD COLUMN IF NOT EXISTS {col};")

        # 다른 테이블 생성
        cur.execute("""
        CREATE TABLE IF NOT EXISTS funding_rates (
            timestamp TIMESTAMPTZ PRIMARY KEY,
            symbol VARCHAR(20) NOT NULL,
            funding_rate NUMERIC
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS open_interest (
            timestamp TIMESTAMPTZ PRIMARY KEY,
            symbol VARCHAR(20) NOT NULL,
            open_interest NUMERIC
        );
        """)

        # klines_1m의 기본 키 확인 및 수정
        cur.execute("""
            SELECT con.conname, a.attname
            FROM pg_catalog.pg_constraint con
            JOIN pg_catalog.pg_attribute a ON a.attnum = ANY(con.conkey)
            WHERE con.conrelid = 'klines_1m'::regclass AND con.contype = 'p';
        """)
        pk_info = cur.fetchall()
        
        pk_cols = {row[1] for row in pk_info}
        pk_name = pk_info[0][0] if pk_info else None

        if pk_cols != {'timestamp', 'symbol'}:
            if pk_name:
                cur.execute(f"ALTER TABLE klines_1m DROP CONSTRAINT IF EXISTS {pk_name};")
            cur.execute("ALTER TABLE klines_1m ADD PRIMARY KEY (timestamp, symbol);")

        conn.commit()
        cur.close()
        logger.info("모든 테이블이 성공적으로 생성되었거나 이미 존재하며, 필요한 스키마 변경이 적용되었습니다.")
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f"테이블 생성 또는 수정 중 오류 발생: {error}")
        if conn:
            conn.rollback()
    finally:
        if conn is not None:
            conn.close()

if __name__ == '__main__':
    print("데이터베이스를 초기화하고 테이블을 생성하는 중...")
    create_tables()
    print("데이터베이스 초기화 완료.")