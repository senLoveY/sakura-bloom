import sys
import json
import time
import requests
import csv
import logging
from io import StringIO
from datetime import date
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.hooks.base import BaseHook
from pendulum import datetime as pen_datetime

sys.path.append('/opt/airflow')
from config.paths_config import MinioConfig, VisualCrossingConfig

logger = logging.getLogger(__name__)

MINIO_CONN_ID = "minio_conn"
VISUALCROSSING_CONN_ID = "visualcrossing_conn"

@dag(
    dag_id="load_weather_data_visualcrossing",
    start_date=pen_datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["ingestion", "historical", "weather"],
)
def load_weather_data():

    @task
    def get_cities_list():
        hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
        bucket = MinioConfig.BUCKET_RAW_DATA
        key = MinioConfig.RAW_BLOOM_DATA_FILE

        if not hook.check_for_key(key=key, bucket_name=bucket):
            logger.warning(f"File {key} not found. Using defaults.")
            return ["Tokyo", "Kyoto"]

        file_content = hook.read_key(key=key, bucket_name=bucket)
        cities = set()
        csv_reader = csv.DictReader(StringIO(file_content))
        for row in csv_reader:
            site_name = row.get('Site Name')
            if site_name:
                cities.add(site_name.strip())

        logger.info(f"Found {len(cities)} cities")
        return list(cities)

    @task(max_active_tis_per_dag=1)
    def fetch_city_history(city_name: str):

        conn = BaseHook.get_connection(VISUALCROSSING_CONN_ID)
        api_key = conn.password
        hook = S3Hook(aws_conn_id=MINIO_CONN_ID)

        start_year = 2000
        current_year = date.today().year

        safe_city = "".join([c for c in city_name if c.isalnum() or c in (' ', '-', '_')]).strip()

        for year in range(start_year, current_year + 1):

            dest_key = f"weather/{safe_city}/{year}_weather.json"
            if hook.check_for_key(key=dest_key, bucket_name=MinioConfig.BUCKET_WEATHER_DATA):
                logger.info(f"Year {year} for {city_name} already exists. Skipping.")
                continue

            start_date = f"{year}-01-01"
            end_date = f"{year}-12-31" if year < current_year else date.today().strftime("%Y-%m-%d")

            location = f"{city_name},{VisualCrossingConfig.COUNTRY}"
            url = f"{VisualCrossingConfig.BASE_URL}/{location}/{start_date}/{end_date}"

            api_params = {
                "unitGroup": VisualCrossingConfig.UNIT_GROUP,
                "contentType": "json",
                "include": "days",
                "key": api_key,
                "elements": "tempmax,tempmin,temp,precip,snow,snowdepth,windspeed"
            }

            try:
                logger.info(f"Requesting: {city_name} year {year}")
                response = requests.get(url, params=api_params, timeout=60)

                if response.status_code == 429:
                    logger.warning("API daily limit reached! Stopping for today.")
                    return

                response.raise_for_status()
                data = response.json()

                hook.load_string(
                    string_data=json.dumps(data, ensure_ascii=False, indent=2),
                    key=dest_key,
                    bucket_name=MinioConfig.BUCKET_WEATHER_DATA,
                    replace=True
                )

                logger.info(f"Successfully saved {year} for {city_name}")

                time.sleep(1)

            except Exception as e:
                logger.error(f"Error for {city_name} in {year}: {e}")
                raise e

    cities = get_cities_list()
    fetch_city_history.expand(city_name=cities)

load_weather_data_dag = load_weather_data()