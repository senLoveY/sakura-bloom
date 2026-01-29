import os
import pandas as pd
import json
import logging
import sys
from io import StringIO
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.trigger_rule import TriggerRule
from pendulum import datetime as pen_datetime

sys.path.append('/opt/airflow')
from config.paths_config import MinioConfig

logger = logging.getLogger(__name__)

MINIO_CONN_ID = "minio_conn"
SNOWFLAKE_CONN_ID = "snowflake_conn"

@dag(
    dag_id="weather_minio_to_snowflake_bulk_load",
    start_date=pen_datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["weather", "snowflake", "bulk_load"],
)
def weather_to_snowflake():

    @task
    def list_weather_files():
        """Get list of JSON files from MinIO."""
        s3 = S3Hook(aws_conn_id=MINIO_CONN_ID)
        keys = s3.list_keys(bucket_name=MinioConfig.BUCKET_WEATHER_DATA, prefix='weather/')
        return [k for k in keys if k.endswith('.json')] if keys else []

    @task
    @task
    def process_and_upload_to_stage(file_key: str):
        s3 = S3Hook(aws_conn_id=MINIO_CONN_ID)
        sn = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

        content = s3.read_key(file_key, bucket_name=MinioConfig.BUCKET_WEATHER_DATA)
        data = json.loads(content)

        import re
        file_name = os.path.basename(file_key) # '2000_weather.json'
        year_match = re.search(r'(\d{4})', file_name)
        year = year_match.group(1) if year_match else "2000"

        base_info = {
            'city_name_en': data.get('address'),
            'resolved_address': data.get('resolvedAddress'),
            'latitude': data.get('latitude'),
            'longitude': data.get('longitude')
        }

        days_list = data.get('days', [])
        if not days_list:
            return None

        df = pd.DataFrame(days_list)

        df['datetime'] = pd.date_range(start=f"{year}-01-01", periods=len(df), freq='D')
        df['datetime'] = df['datetime'].dt.strftime('%Y-%m-%d')

        required_columns = ['tempmax', 'tempmin', 'temp', 'precip', 'snow', 'snowdepth', 'windspeed']
        for col in required_columns:
            if col not in df.columns:
                df[col] = None

        for key, value in base_info.items():
            df[key] = value

        final_df = df[[
            'city_name_en', 'resolved_address', 'latitude', 'longitude',
            'datetime', 'tempmax', 'tempmin', 'temp',
            'precip', 'snow', 'snowdepth', 'windspeed'
        ]]

        local_csv_path = f"/tmp/{file_key.replace('/', '_').replace(' ', '_')}.csv"
        final_df.to_csv(local_csv_path, index=False, header=False, sep='|')

        try:
            put_sql = f"PUT 'file://{local_csv_path}' @SAKURA_DB.RAW.WEATHER_STAGE auto_compress=true"
            sn.run(put_sql)
            return file_key
        finally:
            if os.path.exists(local_csv_path):
                os.remove(local_csv_path)

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def copy_into_historical_table(dummy_input):
        """Final direct COPY INTO from Stage to Structured Table."""
        sn_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

        copy_sql = """
            COPY INTO SAKURA_DB.RAW.WEATHER_HISTORICAL (
                city_name_en, resolved_address, latitude, longitude,
                weather_date, temp_max, temp_min, temp, 
                precip, snow, snow_depth, wind_speed
            )
            FROM @SAKURA_DB.RAW.WEATHER_STAGE
            FILE_FORMAT = (
                TYPE = 'CSV' 
                FIELD_DELIMITER = '|'
                FIELD_OPTIONALLY_ENCLOSED_BY = '"' 
                NULL_IF = ('', 'None', 'nan')
            )
            PURGE = TRUE;
        """

        sn_hook.run(copy_sql)
        logger.info("Direct COPY INTO finished successfully.")

    files = list_weather_files()
    put_tasks = process_and_upload_to_stage.expand(file_key=files)
    copy_into_historical_table(put_tasks)

weather_to_snowflake()