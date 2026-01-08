import os
import json
import pandas as pd
import sys
from airflow.sdk import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk.bases.hook import BaseHook
from airflow.datasets import Dataset
from pendulum import datetime as pen_datetime

sys.path.append('/opt/airflow')
try:
    from config.paths_config import MinioConfig
except ImportError:
    class MinioConfig:
        BUCKET_RAW_DATA = "sakura-bloom-data"
        KAGGLE_PREFIX = "kaggle"
        DATASET_URI = "s3://sakura-bloom-data/kaggle"

KAGGLE_ASSET = Dataset(MinioConfig.DATASET_URI)


@dag(
    dag_id="kaggle_to_minio",
    start_date=pen_datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=['kaggle', 'minio', 'ingestion'],
    description="Downloads Kaggle dataset, splits by years, uploads to MinIO."
)
def sakura_bloom_kaggle_to_minio():
    @task
    def download_dataset_from_kaggle():
        """Downloads the sakura dataset from Kaggle to a temp directory."""
        conn = BaseHook.get_connection("kaggle_conn")

        kaggle_config_dir = "/tmp/.kaggle"
        os.makedirs(kaggle_config_dir, exist_ok=True)

        kaggle_json_path = os.path.join(kaggle_config_dir, "kaggle.json")
        with open(kaggle_json_path, "w") as f:
            json.dump({"username": conn.login, "key": conn.password}, f)
        os.chmod(kaggle_json_path, 0o600)

        os.environ["KAGGLE_CONFIG_DIR"] = kaggle_config_dir

        from kaggle.api.kaggle_api_extended import KaggleApi

        api = KaggleApi()
        api.authenticate()

        dataset_name = "ryanglasnapp/japanese-cherry-blossom-data"
        download_path = "/tmp/sakura_bloom_kaggle_raw"

        if os.path.exists(download_path):
            for f in os.listdir(download_path):
                os.remove(os.path.join(download_path, f))
        os.makedirs(download_path, exist_ok=True)

        api.dataset_download_files(dataset_name, path=download_path, unzip=True)
        return download_path

    @task
    def split_files_by_date_ranges(download_path: str):
        """Splits CSV files into defined year ranges and saves them locally."""
        FILES = {
            "first_bloom": "sakura_first_bloom_dates.csv",
            "full_bloom": "sakura_full_bloom_dates.csv",
        }

        YEAR_RANGES = {
            "1953_1989": range(1953, 1990),
            "1990_2009": range(1990, 2010),
            "2010_2025": range(2010, 2026),
        }

        output_dir = "/tmp/sakura_bloom_splitted"
        if os.path.exists(output_dir):
            for f in os.listdir(output_dir):
                os.remove(os.path.join(output_dir, f))
        os.makedirs(output_dir, exist_ok=True)

        saved_files = []

        for prefix, filename in FILES.items():
            path = os.path.join(download_path, filename)
            if not os.path.exists(path):
                continue

            df = pd.read_csv(path)

            meta_cols = [c for c in df.columns if not c.isdigit()]

            for range_name, years in YEAR_RANGES.items():
                year_cols = [str(y) for y in years if str(y) in df.columns]
                if not year_cols: continue

                subset = df[meta_cols + year_cols]
                out_filename = f"{prefix}_{range_name}.csv"
                out_path = os.path.join(output_dir, out_filename)

                subset.to_csv(out_path, index=False)
                saved_files.append(out_path)

        return saved_files

    @task(outlets=[KAGGLE_ASSET])
    def upload_to_minio(files: list[str]):
        """Uploads split files to MinIO directly under 'kaggle/' prefix."""
        s3 = S3Hook(aws_conn_id="minio_conn")
        bucket = MinioConfig.BUCKET_RAW_DATA
        prefix = MinioConfig.KAGGLE_PREFIX

        uploaded_keys = []
        for file_path in files:
            file_name = os.path.basename(file_path)
            key = f"{prefix}/{file_name}"

            s3.load_file(
                filename=file_path,
                key=key,
                bucket_name=bucket,
                replace=True,
            )
            uploaded_keys.append(key)

        print(f"Uploaded: {uploaded_keys}")
        return uploaded_keys

    raw_path = download_dataset_from_kaggle()
    splitted_files = split_files_by_date_ranges(raw_path)
    upload_to_minio(splitted_files)


sakura_bloom_kaggle_to_minio()