import psycopg2
import pandas as pd
from src.utils.logger import get_logger
from src.utils.db_init import get_db_connection

class PostgresUploader:
    """PostgreSQL 데이터베이스에 데이터 업로드를 처리합니다.
    """
    def __init__(self):
        self.logger = get_logger(__name__)
        self.conn = None
        self._connect()

    def _connect(self):
        """PostgreSQL 데이터베이스에 연결을 설정합니다.
        """
        self.conn = get_db_connection()
        if self.conn:
            self.logger.info("PostgresUploader가 데이터베이스에 연결되었습니다.")
        else:
            self.logger.error("PostgresUploader가 데이터베이스에 연결하지 못했습니다.")

    def is_connected(self):
        """업로더가 PostgreSQL에 연결되어 있으면 True를 반환합니다.
        """
        return self.conn is not None

    def get_historical_klines(self, symbol: str, limit: int = 100):
        """데이터베이스에서 과거 kline 데이터를 가져옵니다.
        """
        if not self.is_connected():
            self.logger.error("과거 kline을 가져올 수 없습니다. PostgreSQL에 연결되어 있지 않습니다.")
            return pd.DataFrame()

        # 'klines_1m' 테이블 이름이 여기에 하드코딩되어 있습니다.
        sql = "SELECT * FROM klines_1m WHERE symbol = %s ORDER BY timestamp DESC LIMIT %s"
        
        try:
            # 데이터를 DataFrame으로 가져옵니다.
            df = pd.read_sql(sql, self.conn, params=(symbol, limit))
            
            # 'timestamp' 열이 인덱스로 설정됩니다.
            df.set_index('timestamp', inplace=True)
            
            # timestamp 인덱스를 기준으로 DataFrame을 오름차순으로 정렬합니다.
            df.sort_index(inplace=True)
            
            self.logger.info(f"데이터베이스에서 {symbol}에 대한 {len(df)}개의 과거 kline을 가져왔습니다.")
            return df
        except (Exception, psycopg2.Error) as error:
            self.logger.error(f"과거 kline 데이터를 가져오는 중 오류 발생: {error}")
            return pd.DataFrame()

    def upload_kline_data(self, kline_data: dict):
        """처리된 kline 데이터(지표 포함)를 klines_1m 테이블에 업로드합니다.
        """
        if not self.is_connected():
            self.logger.error("kline 데이터를 업로드할 수 없습니다. PostgreSQL에 연결되어 있지 않습니다.")
            return

        columns = kline_data.keys()
        values = [kline_data[c] for c in columns]
        
        # 기본 및 업데이트 열 목록 생성
        update_cols = [f"{col} = EXCLUDED.{col}" for col in columns if col not in ('timestamp', 'symbol')]
        
        sql = f"""
        INSERT INTO klines_1m ({', '.join(columns)})
        VALUES ({', '.join(['%s'] * len(values))})
        ON CONFLICT (timestamp, symbol) DO UPDATE SET
            {', '.join(update_cols)}
        """
        
        try:
            cur = self.conn.cursor()
            cur.execute(sql, values)
            self.conn.commit()
            self.logger.debug(f"{kline_data['timestamp']}에 {kline_data['symbol']}에 대한 kline 데이터를 업로드했습니다.")
        except (Exception, psycopg2.Error) as error:
            self.logger.error(f"kline 데이터 업로드 중 오류 발생: {error}")

    def upload_funding_rate(self, funding_rate_data: dict):
        """펀딩 비율 데이터를 funding_rates 테이블에 업로드합니다.
        """
        if not self.is_connected():
            self.logger.error("펀딩 비율을 업로드할 수 없습니다. PostgreSQL에 연결되어 있지 않습니다.")
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
            self.logger.debug(f"{funding_rate_data['timestamp']}에 {funding_rate_data['symbol']}에 대한 펀딩 비율을 업로드했습니다.")
        except (Exception, psycopg2.Error) as error:
            self.logger.error(f"펀딩 비율 업로드 중 오류 발생: {error}")

    def upload_open_interest(self, open_interest_data: dict):
        """미결제 약정 데이터를 open_interest 테이블에 업로드합니다.
        """
        if not self.is_connected():
            self.logger.error("미결제 약정을 업로드할 수 없습니다. PostgreSQL에 연결되어 있지 않습니다.")
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
            self.logger.debug(f"{open_interest_data['timestamp']}에 {open_interest_data['symbol']}에 대한 미결제 약정을 업로드했습니다.")
        except (Exception, psycopg2.Error) as error:
            self.logger.error(f"미결제 약정 업로드 중 오류 발생: {error}")

    def close(self):
        """데이터베이스 연결을 닫습니다.
        """
        if self.conn:
            self.conn.close()
            self.logger.info("PostgresUploader 데이터베이스 연결이 닫혔습니다.")

# 사용 예 (테스트 목적)
if __name__ == '__main__':
    uploader = PostgresUploader()
    if uploader.is_connected():
        # 예: 과거 데이터 가져오기
        historical_data = uploader.get_historical_klines('BTCUSDT', limit=5)
        print("과거 데이터:")
        print(historical_data)

        # ... (나머지 사용 예)
        uploader.close()
    else:
        print("PostgreSQL에 연결하지 못했습니다. .env 및 데이터베이스 상태를 확인하십시오.")
