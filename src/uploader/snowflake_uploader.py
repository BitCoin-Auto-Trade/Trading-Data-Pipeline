import snowflake.connector
import logging
import os

def load_parquet_to_snowflake(file_path: str, table_name: str, stage_name: str, file_format: str) -> bool:
    try:
        conn = snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
            role=os.getenv("SNOWFLAKE_ROLE"),
        )
        cursor = conn.cursor()
        filename = os.path.basename(file_path)

        copy_sql = f'''
        COPY INTO {table_name}
        FROM @{stage_name}/{filename}
        FILE_FORMAT = (FORMAT_NAME = {file_format})
        ON_ERROR = 'CONTINUE';
        '''
        cursor.execute(copy_sql)
        logging.info(f"Loaded {file_path} to {table_name}")
        return True
    except Exception as e:
        logging.error(f"Snowflake load failed: {e}")
        return False
    finally:
        if 'conn' in locals():
            conn.close()
