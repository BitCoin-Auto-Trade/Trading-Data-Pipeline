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
            FILE_FORMAT = (FORMAT_NAME = {file_format})
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            """
            cursor.execute(copy_sql)
            logger.info(f"Loaded {s3_key} into Snowflake table {table_name}")
    except Exception as e:
        logger.error(f"Snowflake COPY INTO failed: {e}")
        raise
    finally:
        conn.close()
