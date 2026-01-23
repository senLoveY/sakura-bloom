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
    tags=["ingestion", "visualcrossing", "weather", "minio"],
    description="Download weather data from VisualCrossing API for cities",
)
def load_weather_data():
    
    @task
    def get_cities_list():

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
    def fetch_and_upload_weather(city_name: str):

        try:
            conn = BaseHook.get_connection(VISUALCROSSING_CONN_ID)
            api_key = conn.password  # API key stored in password field
            logger.info(f"Successfully loaded API key from connection '{VISUALCROSSING_CONN_ID}'")
        except Exception as e:
            logger.error(f"Failed to get VisualCrossing connection: {e}")
            raise ValueError(f"Connection '{VISUALCROSSING_CONN_ID}' not found or invalid")
        
        if not api_key:
            logger.error("API key is empty in the connection!")
            raise ValueError("API key is required but not found in connection password field")
        
        hook = S3Hook(aws_conn_id=MINIO_CONN_ID)


        location = f"{city_name},{VisualCrossingConfig.COUNTRY}"
        date_range = f"{VisualCrossingConfig.START_DATE}/{VisualCrossingConfig.END_DATE}"
        
        url = f"{VisualCrossingConfig.BASE_URL}/{location}/{date_range}"
        
        params = {
            "unitGroup": VisualCrossingConfig.UNIT_GROUP,
            "contentType": VisualCrossingConfig.CONTENT_TYPE,
            "include": VisualCrossingConfig.INCLUDE,
            "key": api_key
        }
        

        if hasattr(VisualCrossingConfig, 'ELEMENTS') and VisualCrossingConfig.ELEMENTS:
            params["elements"] = VisualCrossingConfig.ELEMENTS

        headers = {
            "User-Agent": "SakuraBloomProject/1.0 (weather research)",
            "Accept": "application/json"
        }

        try:
            logger.info(f"Requesting weather data for: {city_name}")
            logger.info(f"Date range: {VisualCrossingConfig.START_DATE} to {VisualCrossingConfig.END_DATE}")
            
            response = requests.get(url, params=params, headers=headers, timeout=30)
            response.raise_for_status()
            data = response.json()

            time.sleep(VisualCrossingConfig.DELAY_BETWEEN_REQUESTS)

            if not data:
                logger.warning(f"No weather data returned for '{city_name}'")
                return

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            

            safe_city_name = "".join([c for c in city_name if c.isalnum() or c in (' ', '-', '_')]).strip()

            dest_key = MinioConfig.WEATHER_DATA_PATH.format(
                city=safe_city_name,
                timestamp=timestamp
            )


            json_byte_string = json.dumps(data, ensure_ascii=False, indent=2)


            hook.load_string(
                string_data=json_byte_string,
                key=dest_key,
                bucket_name=MinioConfig.BUCKET_WEATHER_DATA,
                replace=True
            )
            
            logger.info(f"SUCCESS: Saved weather data to {MinioConfig.BUCKET_WEATHER_DATA}/{dest_key}")
            logger.info(f"Retrieved {len(data.get('days', []))} days of weather data for {city_name}")

        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error for {city_name}: {e}")
            logger.error(f"Response: {e.response.text if hasattr(e, 'response') else 'No response'}")

            
        except requests.exceptions.Timeout:
            logger.error(f"Request timeout for {city_name}")

        except requests.exceptions.RequestException as e:
            logger.error(f"Request error for {city_name}: {e}")

        except Exception as e:
            logger.error(f"Unexpected error processing {city_name}: {e}")

    cities_list = get_cities_list()
    fetch_and_upload_weather.expand(city_name=cities_list)


load_weather_data()