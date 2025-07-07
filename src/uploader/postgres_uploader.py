import psycopg2
from src.utils.logger import get_logger
from src.utils.db_init import get_db_connection # Reusing the connection function

class PostgresUploader:
    """Handles data uploading to a PostgreSQL database.
    """
    def __init__(self):
        self.logger = get_logger(__name__)
        self.conn = None
        self._connect()

    def _connect(self):
        """Establishes a connection to the PostgreSQL database.
        """
        self.conn = get_db_connection()
        if self.conn:
            self.logger.info("PostgresUploader connected to database.")
        else:
            self.logger.error("PostgresUploader failed to connect to database.")

    def is_connected(self):
        """Returns True if the uploader is connected to PostgreSQL.
        """
        return self.conn is not None

    def upload_kline_data(self, kline_data: dict):
        """Uploads processed kline data (with indicators) to klines_1m table.
        """
        if not self.is_connected():
            self.logger.error("Cannot upload kline data. PostgreSQL is not connected.")
            return

        sql = """
        INSERT INTO klines_1m (
            timestamp, symbol, open, high, low, close, volume,
            ema_20, rsi_14, macd, macd_signal, macd_hist
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (timestamp) DO UPDATE SET
            open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low, close = EXCLUDED.close, volume = EXCLUDED.volume,
            ema_20 = EXCLUDED.ema_20, rsi_14 = EXCLUDED.rsi_14,
            macd = EXCLUDED.macd, macd_signal = EXCLUDED.macd_signal, macd_hist = EXCLUDED.macd_hist
        """
        try:
            cur = self.conn.cursor()
            cur.execute(sql, (
                kline_data['timestamp'], kline_data['symbol'],
                kline_data['open'], kline_data['high'], kline_data['low'], kline_data['close'], kline_data['volume'],
                kline_data.get('ema_20'), kline_data.get('rsi_14'),
                kline_data.get('macd'), kline_data.get('macd_signal'), kline_data.get('macd_hist')
            ))
            self.conn.commit()
            self.logger.debug(f"Uploaded kline data for {kline_data['symbol']} at {kline_data['timestamp']}")
        except (Exception, psycopg2.Error) as error:
            self.logger.error(f"Error uploading kline data: {error}")

    def upload_funding_rate(self, funding_rate_data: dict):
        """Uploads funding rate data to funding_rates table.
        """
        if not self.is_connected():
            self.logger.error("Cannot upload funding rate. PostgreSQL is not connected.")
            return

        sql = """
        INSERT INTO funding_rates (timestamp, symbol, funding_rate)
        VALUES (%s, %s, %s)
        ON CONFLICT (timestamp) DO UPDATE SET
            funding_rate = EXCLUDED.funding_rate
        """
        try:
            cur = self.conn.cursor()
            cur.execute(sql, (
                funding_rate_data['timestamp'], funding_rate_data['symbol'],
                funding_rate_data['funding_rate']
            ))
            self.conn.commit()
            self.logger.debug(f"Uploaded funding rate for {funding_rate_data['symbol']} at {funding_rate_data['timestamp']}")
        except (Exception, psycopg2.Error) as error:
            self.logger.error(f"Error uploading funding rate: {error}")

    def upload_open_interest(self, open_interest_data: dict):
        """Uploads open interest data to open_interest table.
        """
        if not self.is_connected():
            self.logger.error("Cannot upload open interest. PostgreSQL is not connected.")
            return

        sql = """
        INSERT INTO open_interest (timestamp, symbol, open_interest)
        VALUES (%s, %s, %s)
        ON CONFLICT (timestamp) DO UPDATE SET
            open_interest = EXCLUDED.open_interest
        """
        try:
            cur = self.conn.cursor()
            cur.execute(sql, (
                open_interest_data['timestamp'], open_interest_data['symbol'],
                open_interest_data['open_interest']
            ))
            self.conn.commit()
            self.logger.debug(f"Uploaded open interest for {open_interest_data['symbol']} at {open_interest_data['timestamp']}")
        except (Exception, psycopg2.Error) as error:
            self.logger.error(f"Error uploading open interest: {error}")

    def close(self):
        """Closes the database connection.
        """
        if self.conn:
            self.conn.close()
            self.logger.info("PostgresUploader database connection closed.")

# Example Usage (for testing purposes)
if __name__ == '__main__':
    uploader = PostgresUploader()
    if uploader.is_connected():
        # Example kline data
        kline_example = {
            'timestamp': datetime.datetime.now(),
            'symbol': 'BTCUSDT',
            'open': 100000.0,
            'high': 101000.0,
            'low': 99000.0,
            'close': 100500.0,
            'volume': 100.0,
            'ema_20': 99900.0,
            'rsi_14': 60.0,
            'macd': 100.0,
            'macd_signal': 90.0,
            'macd_hist': 10.0
        }
        uploader.upload_kline_data(kline_example)

        # Example funding rate data
        funding_rate_example = {
            'timestamp': datetime.datetime.now(),
            'symbol': 'BTCUSDT',
            'funding_rate': 0.0001
        }
        uploader.upload_funding_rate(funding_rate_example)

        # Example open interest data
        open_interest_example = {
            'timestamp': datetime.datetime.now(),
            'symbol': 'BTCUSDT',
            'open_interest': 1000000000.0
        }
        uploader.upload_open_interest(open_interest_example)

        uploader.close()
    else:
        print("Failed to connect to PostgreSQL. Check .env and database status.")
