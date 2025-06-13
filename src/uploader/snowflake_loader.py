import os
import pandas as pd
import snowflake.connector
import logging

logger = logging.getLogger(__name__)

def fetch_ohlcv_from_snowflake(symbol: str, interval: str) -> pd.DataFrame:
    """Snowflake에서 OHLCV 데이터를 조회합니다."""
    table_name = f"ohlcv_{interval}"
    try:
        conn = snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
        )

        query = f"""
            SELECT timestamp, open, high, low, close, volume
            FROM {table_name}
            WHERE symbol = %s
            ORDER BY timestamp DESC
            LIMIT 300
        """

        df = pd.read_sql(query, conn, params=(symbol,))
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df = df.sort_values("timestamp")  # 시간 오름차순 정렬
        return df
    except Exception as e:
        logger.exception(f"[Snowflake] {symbol} {interval} 데이터 조회 실패: {e}")
        return pd.DataFrame()
    finally:
        try:
            conn.close()
        except:
            pass
