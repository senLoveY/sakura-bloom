import os
import logging
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from pendulum import datetime as pen_datetime

import sys
sys.path.append('/opt/airflow')
from config.paths_config import MinioConfig

logger = logging.getLogger(__name__)

MINIO_CONN_ID = "minio_conn"
SNOWFLAKE_CONN_ID = "snowflake_conn"

@dag(
    dag_id="geo_minio_to_snowflake_load",
    start_date=pen_datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["snowflake", "copy_into", "bulk_load"],
)
def snowflake_bulk_load():

    @task
    def get_files_from_minio():
        """File list in MinIO"""
        s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
        keys = s3_hook.list_keys(bucket_name=MinioConfig.BUCKET_GEO_DATA)
        return keys if keys else []

    @task
    def upload_to_snowflake_stage(file_key: str):
        """MinIO -> Local -> Snowflake Stage"""
        s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
        sn_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

        local_path = f"/tmp/{os.path.basename(file_key)}"
        file_obj = s3_hook.get_key(file_key, bucket_name=MinioConfig.BUCKET_GEO_DATA)
        file_obj.download_file(local_path)

        try:
            put_sql = f"PUT file://{local_path} @geo_import_stage auto_compress=true"
            sn_hook.run(put_sql)
            logger.info(f"File {file_key} put to @geo_import_stage")
            return local_path
        finally:
            if os.path.exists(local_path):
                os.remove(local_path)

    @task
    @task
    def copy_into_table(dummy_input):
        sn_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

        copy_sql = """
            COPY INTO SAKURA_DB.RAW.GEOLOCATION_CLEAN (
                place_id, name, display_name, lat, lon, place_type
            )
            FROM (
              SELECT 
                $1:place_id::number,
                $1:name::string,
                $1:display_name::string,
                $1:lat::float,
                $1:lon::float,
                $1:type::string
              FROM @SAKURA_DB.RAW.GEO_IMPORT_STAGE
            )
            FILE_FORMAT = (
                TYPE = 'JSON' 
                STRIP_OUTER_ARRAY = TRUE
            )
            ON_ERROR = 'CONTINUE';
        """

        sn_hook.run(copy_sql)

        sn_hook.run("REMOVE @SAKURA_DB.RAW.GEO_IMPORT_STAGE")
        logger.info("Structured data loaded and stage cleaned")


    files = get_files_from_minio()
    put_tasks = upload_to_snowflake_stage.expand(file_key=files)
    copy_into_table(put_tasks)

snowflake_bulk_load()