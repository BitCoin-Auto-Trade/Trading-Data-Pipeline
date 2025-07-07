import os
import psycopg2
from dotenv import load_dotenv
from src.utils.logger import get_logger

load_dotenv()
logger = get_logger(__name__)

def get_db_connection():
    """Establishes a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            database=os.getenv("POSTGRES_DB", "postgres"),
            user=os.getenv("POSTGRES_USER", "postgres"),
            password=os.getenv("POSTGRES_PASSWORD", ""),
            port=os.getenv("POSTGRES_PORT", 5432)
        )
        logger.info("Database connection established successfully.")
        return conn
    except psycopg2.OperationalError as e:
        logger.error(f"Could not connect to the database: {e}")
        return None

def create_tables():
    """Create tables in the PostgreSQL database."""
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
        """
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
        logger.info("All tables created successfully or already exist.")
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f"Error while creating tables: {error}")
    finally:
        if conn is not None:
            conn.close()

if __name__ == '__main__':
    print("Initializing database and creating tables...")
    create_tables()
    print("Database initialization complete.")