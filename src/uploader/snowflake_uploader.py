import logging
from typing import Optional

logger = logging.getLogger(__name__)

def load_parquet_to_snowflake(
    s3_key: str,
    table_name: str,
    stage_name: str,
    file_format: str,
    conn: Optional[object] = None,
) -> None:
    """Snowflake에 S3 Parquet 파일 적재"""
    import os
    import snowflake.connector

    owns_connection = False
    if conn is None:
        conn = snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
        )
        owns_connection = True

    try:
        sql = f"""
            MERGE INTO {table_name} AS target
            USING (
                SELECT 
                    $1:timestamp::TIMESTAMP AS timestamp,
                    $1:open::FLOAT AS open,
                    $1:high::FLOAT AS high,
                    $1:low::FLOAT AS low,
                    $1:close::FLOAT AS close,
                    $1:volume::FLOAT AS volume,
                    $1:symbol::STRING AS symbol
                FROM @{stage_name}/{s3_key} (FILE_FORMAT => '{file_format}')
            ) AS source
            ON target.timestamp = source.timestamp AND target.symbol = source.symbol
            WHEN MATCHED THEN UPDATE SET
                open = source.open,
                high = source.high,
                low = source.low,
                close = source.close,
                volume = source.volume
            WHEN NOT MATCHED THEN INSERT (
                timestamp, open, high, low, close, volume, symbol
            ) VALUES (
                source.timestamp, source.open, source.high, source.low, source.close, source.volume, source.symbol
            )
        """

        logger.info(f"[Snowflake MERGE] {sql}")

        with conn.cursor() as cursor:
            cursor.execute(sql)
            try:
                result = cursor.fetchall()
                for row in result:
                    logger.info(f"[Snowflake Result] {row}")
            except Exception:
                pass  # fetchall()이 없는 경우도 있어 무시

    except Exception as e:
        logger.exception(f"Snowflake MERGE 실패: {e}")
        raise
    finally:
        if owns_connection and conn:
            conn.close()


def load_to_snowflake(s3_path: str, table: str) -> None:
    """S3 경로를 기반으로 Snowflake MERGE 수행"""
    import os
    stage = os.getenv("SNOWFLAKE_STAGE")
    file_format = os.getenv("SNOWFLAKE_FILE_FORMAT")

    if not stage or not file_format:
        raise RuntimeError("환경변수 SNOWFLAKE_STAGE 또는 SNOWFLAKE_FILE_FORMAT가 누락됨")

    load_parquet_to_snowflake(
        s3_key=s3_path,
        table_name=table,
        stage_name=stage,
        file_format=file_format
    )
