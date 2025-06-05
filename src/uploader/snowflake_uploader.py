import logging
import os
import snowflake.connector

logger = logging.getLogger(__name__)

def load_parquet_to_snowflake(
    s3_key: str,
    table_name: str,
    stage_name: str,
    file_format: str,
    conn=None,
) -> None:
    conn = conn or snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
    )

    try:
        with conn.cursor() as cursor:
            copy_sql = f"""
            COPY INTO {table_name}
            FROM @{stage_name}/{s3_key}
            FILE_FORMAT = (FORMAT_NAME = '{file_format}')
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            """
            logger.info(f"[COPY SQL] {copy_sql.strip()}")
            cursor.execute(copy_sql)

            result = cursor.fetchall()
            for row in result:
                logger.info(f"[COPY RESULT] {row}")
    except Exception as e:
        logger.error(f"Snowflake COPY INTO failed: {e}")
    finally:
        conn.close()


def load_to_snowflake(s3_path: str, table: str) -> None:
    stage = os.getenv("SNOWFLAKE_STAGE")
    file_format = os.getenv("SNOWFLAKE_FILE_FORMAT")

    if not stage or not file_format:
        raise RuntimeError("SNOWFLAKE_STAGE 또는 SNOWFLAKE_FILE_FORMAT 환경변수가 없음")

    load_parquet_to_snowflake(
        s3_key=s3_path,
        table_name=table,
        stage_name=stage,
        file_format=file_format
    )
