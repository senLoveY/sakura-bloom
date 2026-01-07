from airflow.sdk import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk.bases.hook import BaseHook
import os
import json
import pandas as pd

@dag(
    description='Download a Kaggle dataset from Kaggle and load it to MinIO',
    schedule=None,
    catchup=False,
    tags=['kaggle', 'minio'],
)

def sakura_bloom_kaggle_to_minio():
    @task
    def download_dataset_from_kaggle():

        conn = BaseHook.get_connection("kaggle_conn")

        kaggle_config_dir = "/tmp/.kaggle"
        os.makedirs(kaggle_config_dir, exist_ok=True)
        kaggle_json_path = os.path.join(kaggle_config_dir, "kaggle.json")

        with open(kaggle_json_path, "w") as f:
            json.dump({
                "username": conn.login,
                "key": conn.password
            }, f)
        os.chmod(kaggle_json_path, 0o600)

        os.environ["KAGGLE_CONFIG_DIR"] = kaggle_config_dir

        from kaggle.api.kaggle_api_extended import KaggleApi

        api = KaggleApi()
        api.authenticate()

        dataset_name = "ryanglasnapp/japanese-cherry-blossom-data"
        download_path = "/tmp/sakura_bloom_kaggle_dataset"
        os.makedirs(download_path, exist_ok=True)
        api.dataset_download_files(dataset_name, path=download_path, unzip=True)

        print(f"Downloaded {len(os.listdir(download_path))} CSV files: {os.listdir(download_path)}")
        return download_path

    @task
    def split_files_by_date_ranges(download_path: str):
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
        os.makedirs(output_dir, exist_ok=True)

        saved_files = []

        for prefix, filename in FILES.items():
            path = os.path.join(download_path, filename)
            df = pd.read_csv(path)

            meta_cols = [c for c in df.columns if not c.isdigit()]

            for range_name, years in YEAR_RANGES.items():
                year_cols = [str(y) for y in years if str(y) in df.columns]

                subset = df[meta_cols + year_cols]

                out_file = f"{prefix}_{range_name}.csv"
                out_path = os.path.join(output_dir, out_file)

                subset.to_csv(out_path, index=False)
                saved_files.append(out_path)

        return saved_files

    @task
    def upload_dataset_to_minio(files: list[str]):
        s3 = S3Hook(aws_conn_id="minio_conn")
        uploaded = []
        for file_path in files:
            key = f"sakura_bloom_dataset/{os.path.basename(file_path)}"
            s3.load_file(
                filename=file_path,
                key=key,
                bucket_name="sakura-bloom",
                replace=True,
            )
            uploaded.append(key)

        return uploaded


    download_path = download_dataset_from_kaggle()
    split_files = split_files_by_date_ranges(download_path)
    upload_dataset_to_minio(split_files)


sakura_bloom_kaggle_to_minio()