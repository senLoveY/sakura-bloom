import logging
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from pendulum import datetime


@dag(
    dag_id="init_minio_buckets",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["infrastructure", "setup"],
    description="Creates a bucket structure in MinIO according to the system design"
)
def init_buckets():
    @task
    def create_buckets_if_not_exist():
        buckets_to_create = [
            "sakura-bloom-data",
            "geo-data",
            "weather-data"
        ]

        hook = S3Hook(aws_conn_id="minio_conn")

        for bucket in buckets_to_create:
            if not hook.check_for_bucket(bucket_name=bucket):
                hook.create_bucket(bucket_name=bucket)
                logging.info(f"bucket '{bucket}' successfully created")
            else:
                logging.info(f"bucket '{bucket}' already exists")

    create_buckets_if_not_exist()


init_buckets()