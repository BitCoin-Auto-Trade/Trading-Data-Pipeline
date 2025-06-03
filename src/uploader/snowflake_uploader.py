def load_parquet_to_snowflake(s3_key: str, table_name: str, stage_name: str, file_format: str):
    try:
        import snowflake.connector
        import logging
        import os

        conn = snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
        )

        cursor = conn.cursor()
        copy_sql = f"""
        COPY INTO {table_name}
        FROM @{stage_name}/{s3_key}
        FILE_FORMAT = (FORMAT_NAME = {file_format})
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        """
        cursor.execute(copy_sql)
        logging.info(f"Loaded parquet from {s3_key} into {table_name}")
        cursor.close()
        conn.close()
    except Exception as e:
        logging.error(f"Snowflake load failed: {e}")
        raise
