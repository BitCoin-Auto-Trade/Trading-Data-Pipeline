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
    commands = (
        """
        CREATE TABLE IF NOT EXISTS klines_1m (
            timestamp TIMESTAMPTZ PRIMARY KEY,
            symbol VARCHAR(20) NOT NULL,
            open NUMERIC, high NUMERIC, low NUMERIC, close NUMERIC, volume NUMERIC,
            ema_20 NUMERIC, rsi_14 NUMERIC,
            macd NUMERIC, macd_signal NUMERIC, macd_hist NUMERIC
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS funding_rates (
            timestamp TIMESTAMPTZ PRIMARY KEY,
            symbol VARCHAR(20) NOT NULL,
            funding_rate NUMERIC
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS open_interest (
            timestamp TIMESTAMPTZ PRIMARY KEY,
            symbol VARCHAR(20) NOT NULL,
            open_interest NUMERIC
        )
        """,
        "ALTER TABLE klines_1m ADD COLUMN IF NOT EXISTS atr_14 NUMERIC;"
    )
    
    conn = None
    try:
        conn = get_db_connection()
        if conn is None:
            return
            
        cur = conn.cursor()
        for command in commands:
            cur.execute(command)
        
        cur.close()
        conn.commit()
        logger.info("모든 테이블이 성공적으로 생성되었거나 이미 존재하며, 필요한 스키마 변경이 적용되었습니다.")
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f"테이블 생성 또는 수정 중 오류 발생: {error}")
    finally:
        if conn is not None:
            conn.close()

if __name__ == '__main__':
    print("데이터베이스를 초기화하고 테이블을 생성하는 중...")
    create_tables()
    print("데이터베이스 초기화 완료.")