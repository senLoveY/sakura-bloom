#!/usr/bin/env python3

from datetime import datetime, timedelta
import json
import requests
from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.standard.operators.python import PythonOperator


API_KEY = ""
BASE_URL = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline"
MINIO_BUCKET = "weather-data"
MINIO_CONN_ID = "minio_conn"
SNOWFLAKE_CONN_ID = "snowflake_default"

DEFAULT_CITIES = [
    {"city_id": "1", "city_name": "London,UK"},
]


def get_cities_from_db(**context):
    try:
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

        sql = """
              SELECT DISTINCT LOCATION_ID  as city_id, \
                              CITY_NAME_EN as city_name
              FROM SAKURA_DB.RAW.RAW_SAKURA_BLOOM
              WHERE CITY_NAME_EN IS NOT NULL LIMIT 50 \
              """

        result = hook.get_records(sql)

        if result and len(result) > 0:
            cities = [{"city_id": row[0], "city_name": row[1]} for row in result]
            print(f"Found {len(cities)} cities from database")
        else:
            cities = DEFAULT_CITIES
            print(f"No cities in database, using {len(cities)} default cities")

    except Exception as e:
        print(f"Error fetching cities from DB: {e}")
        print(f"Using {len(DEFAULT_CITIES)} default cities")
        cities = DEFAULT_CITIES

    ti = context['ti']
    ti.xcom_push(key='cities', value=cities)
    print(f"Cities to process: {[c['city_name'] for c in cities]}")

    return cities


def generate_date_list(**context):
    dag_run = context['dag_run']

    start_date_param = dag_run.conf.get('start_date') if dag_run.conf else None
    end_date_param = dag_run.conf.get('end_date') if dag_run.conf else None

    if start_date_param and end_date_param:
        start_date = datetime.strptime(start_date_param, '%Y-%m-%d')
        end_date = datetime.strptime(end_date_param, '%Y-%m-%d')
        print(f"Using date range from params: {start_date_param} to {end_date_param}")
    else:
        logical_date = context.get('logical_date') or context.get('data_interval_start') or datetime.now()
        start_date = logical_date
        end_date = logical_date
        print(f"Using logical date: {start_date.strftime('%Y-%m-%d')}")

    date_list = []
    current_date = start_date

    while current_date <= end_date:
        date_list.append(current_date.strftime('%Y-%m-%d'))
        current_date += timedelta(days=1)

    print(f"Total dates to process: {len(date_list)}")
    print(f"Date range: {date_list[0]} to {date_list[-1]}")

    ti = context['ti']
    ti.xcom_push(key='date_list', value=date_list)

    return date_list


def fetch_weather_data(city_name, date_str=None, **context):
    if date_str is None:
        date_str = datetime.now().strftime('%Y-%m-%d')

    url = f"{BASE_URL}/{city_name}/{date_str}"
    params = {
        "key": API_KEY,
        "unitGroup": "metric",
        "include": "hours",
        "contentType": "json"
    }

    print(f"Fetching weather for {city_name} on {date_str}")

    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()

        data = response.json()

        print(f"Success! Location: {data.get('resolvedAddress')}")
        if 'days' in data and len(data['days']) > 0:
            first_day = data['days'][0]
            hours_count = len(first_day.get('hours', []))
            print(f"Retrieved {hours_count} hourly records")

        return data

    except requests.exceptions.RequestException as e:
        print(f"Error fetching weather data: {e}")
        raise


def download_and_save_to_minio(**context):
    ti = context['ti']

    cities = ti.xcom_pull(key='cities', task_ids='get_cities')
    date_list = ti.xcom_pull(key='date_list', task_ids='generate_dates')

    if not cities:
        raise ValueError("No cities found in XCom")

    if not date_list:
        raise ValueError("No dates found in XCom")

    s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)

    if not s3_hook.check_for_bucket(MINIO_BUCKET):
        s3_hook.create_bucket(MINIO_BUCKET)
        print(f"Created bucket: {MINIO_BUCKET}")

    uploaded_files = []
    total_operations = len(cities) * len(date_list)
    current_operation = 0

    for date_str in date_list:
        print(f"\nProcessing date: {date_str}")

        for city in cities:
            current_operation += 1
            city_id = city['city_id']
            city_name = city['city_name']

            print(f"[{current_operation}/{total_operations}] {city_name} - {date_str}")

            safe_city_name = city_name.replace(',', '_').replace(' ', '_').replace('/', '_')

            try:
                weather_data = fetch_weather_data(city_name, date_str)

                if 'days' not in weather_data or len(weather_data['days']) == 0:
                    print(f"No daily data for {city_name}")
                    continue

                day_data = weather_data['days'][0]

                if 'hours' not in day_data or len(day_data['hours']) == 0:
                    print(f"No hourly data for {city_name}")
                    continue

                hours = day_data['hours']

                for hour_data in hours:
                    hour_time = hour_data.get('datetime', '00:00:00')

                    datetime_str = f"{date_str}_{hour_time.replace(':', '-')}"

                    hourly_json = {
                        'city_id': city_id,
                        'city_name': city_name,
                        'location': weather_data.get('resolvedAddress'),
                        'latitude': weather_data.get('latitude'),
                        'longitude': weather_data.get('longitude'),
                        'timezone': weather_data.get('timezone'),
                        'date': date_str,
                        'datetime': hour_time,
                        'weather': hour_data,
                        '_metadata': {
                            'fetched_at': datetime.now().isoformat(),
                            'source': 'visualcrossing'
                        }
                    }

                    s3_key = f"visualcrossing/{safe_city_name}/{datetime_str}.json"

                    json_data = json.dumps(hourly_json, indent=2, ensure_ascii=False)

                    s3_hook.load_string(
                        string_data=json_data,
                        key=s3_key,
                        bucket_name=MINIO_BUCKET,
                        replace=True
                    )

                    uploaded_files.append({
                        'city_id': city_id,
                        'city_name': city_name,
                        's3_key': s3_key,
                        'date': date_str,
                        'hour': hour_time
                    })

                print(f"Uploaded {len(hours)} hourly files for {city_name} on {date_str}")

            except Exception as e:
                print(f"Error processing {city_name} on {date_str}: {e}")
                continue

    print(f"\nSummary: Successfully uploaded {len(uploaded_files)} hourly files to MinIO")
    print(f"Processed {len(date_list)} dates for {len(cities)} cities")

    ti.xcom_push(key='uploaded_files', value=uploaded_files)

    return uploaded_files


def load_from_minio_to_snowflake(**context):
    ti = context['ti']

    uploaded_files = ti.xcom_pull(key='uploaded_files', task_ids='download_and_save')

    if not uploaded_files:
        print("No files to load")
        return

    s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
    snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

    loaded_count = 0

    for file_info in uploaded_files:
        s3_key = file_info['s3_key']
        city_name = file_info['city_name']
        hour = file_info.get('hour', 'N/A')

        try:
            json_content = s3_hook.read_key(
                key=s3_key,
                bucket_name=MINIO_BUCKET
            )

            hourly_data = json.loads(json_content)

            sql = """
                  INSERT INTO SAKURA_DB.RAW.RAW_WEATHER_JSON (WEATHER_DATA)
                  SELECT PARSE_JSON(%s) \
                  """

            snowflake_hook.run(
                sql=sql,
                parameters=[json_content]
            )

            loaded_count += 1

            if loaded_count % 100 == 0:
                print(f"Loaded {loaded_count} files...")

        except Exception as e:
            print(f"Error loading {s3_key}: {e}")
            continue

    print(f"Summary: Successfully loaded {loaded_count}/{len(uploaded_files)} hourly files to Snowflake")

    return loaded_count


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        dag_id='weather_data_pipeline',
        default_args=default_args,
        description='Weather Data Pipeline: Visual Crossing API to MinIO to Snowflake',
        schedule='0 2 * * *',
        start_date=datetime(2025, 1, 1),
        catchup=False,
        tags=['weather', 'etl', 'minio', 'snowflake'],
        params={
            "start_date": "2026-01-20",
            "end_date": "2026-01-20"
        }
) as dag:
    get_cities = PythonOperator(
        task_id='get_cities',
        python_callable=get_cities_from_db,
    )

    generate_dates = PythonOperator(
        task_id='generate_dates',
        python_callable=generate_date_list,
    )

    download_and_save = PythonOperator(
        task_id='download_and_save',
        python_callable=download_and_save_to_minio,
    )

    load_to_snowflake = PythonOperator(
        task_id='load_to_snowflake',
        python_callable=load_from_minio_to_snowflake,
    )

    get_cities >> generate_dates >> download_and_save >> load_to_snowflake