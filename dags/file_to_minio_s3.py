from airflow.sdk import dag
from airflow.providers.amazon.aws.transfers.local_to_s3 \
    import LocalFilesystemToS3Operator


@dag(
    description='Upload a local file to MinIO S3',
    schedule=None,
    catchup=False,
)
def upload_local_file_to_minio():
    upload_task = LocalFilesystemToS3Operator(
        task_id='upload_file',
        filename='/opt/airflow/dags/example.csv',
        dest_bucket='miniobucket',
        dest_key='example.csv',
        aws_conn_id='minio_conn',  # Connection to MinIO
    )


upload_local_file_to_minio()
