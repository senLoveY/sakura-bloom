# Airflow with MinIO Upload DAG

This repository contains a Docker Compose setup for Apache Airflow with a DAG to upload local files to MinIO S3.

## Setup

1. Clone this repository.
2. Ensure Docker and Docker Compose are installed.
3. Run `docker-compose up -d` to start the services.
4. Access Airflow UI at http://localhost:8080 (default credentials: airflow/airflow).

## Adding the DAG

```python
# Configuration constants for the upload DAG

LOCAL_FILE_PATH = '/opt/airflow/dags/example.txt'
DEST_BUCKET = 'my-bucket'
DEST_KEY = 'uploaded_file.txt'
```

Then, create `dags/upload_to_minio.py`:

```python
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
        filename='/opt/airflow/dags/<your_filename>.csv',
        dest_bucket='<bucket_name>',
        dest_key='<uploaded_filename>.csv',
        aws_conn_id='minio_conn',  # Connection to MinIO
    )


upload_local_file_to_minio()

```

## MinIO Connection Setup

In the Airflow UI, create a connection:

- **Connection Id**: `minio_conn`
- **Connection Type**: Amazon Web Services
- **AWS Access Key ID**: Your MinIO access key ("minioadmin" if optional step 2 in Usage is used)
- **AWS Secret Access Key**: Your MinIO secret key ("minioadmin" if optional step 2 in Usage is used)
- **Extra**: `{"endpoint_url": "http://your-minio-endpoint:9000"}`

## Usage

1. Start the services with `docker-compose up -d`.
2. [Optional] Create an isolated container for MinIO using the command:

    ```bash
    docker run -p 9000:9000 -p 9001:9001 \
    -e MINIO_ROOT_USER=minioadmin \
    -e MINIO_ROOT_PASSWORD=minioadmin \
    minio/minio server /data --console-address ":9001"
    ```

3. Place your file in the mounted `dags/` directory (or adjust the `local_file` parameter).
4. Hardcode filename, dest_bucket and dest_key directly in DAG's task (see DAG python code above).
5. Trigger the DAG manually from the Airflow UI, providing the desired parameters.

## Notes

- The `dags/`, `logs/`, `config/`, and `plugins/` directories are created automatically by the Docker Compose setup.
- Test data files (`*.txt`, `*.csv`, `*.json`) are ignored by `.gitignore` to prevent accidental commits.
- Database data persists in a Docker named volume across container restarts.
