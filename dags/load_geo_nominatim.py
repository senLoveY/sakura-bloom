import sys
import os
import json
import time
import requests
import csv
import logging
from io import StringIO
from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from pendulum import datetime as pen_datetime

sys.path.append('/opt/airflow')
from config.paths_config import MinioConfig

logger = logging.getLogger(__name__)

MINIO_CONN_ID = "minio_conn"


@dag(
    dag_id="load_geo_data_nominatim",
    start_date=pen_datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["ingestion", "nominatim", "geo", "minio"],
    description="download geo data from Nominatim",
)
def load_geo_data():
    @task
    def get_cities_list():
        """
        read CSV from MinIO and return list of cities
        """
        hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
        bucket = MinioConfig.BUCKET_RAW_DATA
        key = MinioConfig.RAW_BLOOM_DATA_FILE

        if not hook.check_for_key(key=key, bucket_name=bucket):
            logger.warning(f"File {key} not found in {bucket}. Returning default list.")
            return ["Tokyo", "Kyoto"]

        file_content = hook.read_key(key=key, bucket_name=bucket)

        cities = set()
        csv_reader = csv.DictReader(StringIO(file_content))

        logger.info(f"CSV Fieldnames: {csv_reader.fieldnames}")

        for row in csv_reader:
            site_name = row.get('Site Name')
            if site_name:
                cities.add(site_name.strip())

        logger.info(f"Found {len(cities)} unique cities")
        return list(cities)

    @task(max_active_tis_per_dag=1)
    def fetch_and_upload_city(city_name: str):
        """
        Requests data in Nominatim and saves JSON in MinIO.
        """
        hook = S3Hook(aws_conn_id=MINIO_CONN_ID)

        url = "https://nominatim.openstreetmap.org/search"
        params = {
            "q": f"{city_name}, Japan",
            "format": "json",
            "limit": 1
        }
        headers = {
            "User-Agent": "SakuraBloomProject/1.0 (internal research)",
            "Accept-Language": "en-US,en;q=0.9"
        }

        try:
            logger.info(f"Requesting geodata for: {city_name}")
            response = requests.get(url, params=params, headers=headers)
            response.raise_for_status()
            data = response.json()

            time.sleep(1.1)

            if not data:
                logger.warning(f"Data for '{city_name}' was not found in Nominatim.")
                return

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            safe_city_name = "".join([c for c in city_name if c.isalnum() or c in (' ', '-', '_')]).strip()

            dest_key = MinioConfig.GEO_NOMINATIM_PATH.format(
                city=safe_city_name,
                timestamp=timestamp
            )

            json_byte_string = json.dumps(data, ensure_ascii=False, indent=2)

            hook.load_string(
                string_data=json_byte_string,
                key=dest_key,
                bucket_name=MinioConfig.BUCKET_GEO_DATA,
                replace=True
            )
            logger.info(f"SUCCESS: Saved in {MinioConfig.BUCKET_GEO_DATA}/{dest_key}")

        except Exception as e:
            logger.error(f"Processing error for {city_name}: {e}")
            raise e

    cities_list = get_cities_list()
    fetch_and_upload_city.expand(city_name=cities_list)


load_geo_data()