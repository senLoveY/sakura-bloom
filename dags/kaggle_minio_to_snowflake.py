import os
import pandas as pd
import uuid
import logging
import sys
from io import StringIO
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from pendulum import datetime as pen_datetime

sys.path.append('/opt/airflow')
from config.paths_config import MinioConfig

logger = logging.getLogger(__name__)

MINIO_CONN_ID = "minio_conn"
SNOWFLAKE_CONN_ID = "snowflake_conn"

@dag(
    dag_id="kaggle_minio_to_snowflake_load",
    start_date=pen_datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=['kaggle', 'snowflake', 'full_data'],
)
def sakura_full_transposition_pipeline():

    @task
    def process_and_transform():
        """
        Reads CSVs, preserves all metadata columns, melts year columns,
        and merges first/full bloom dates.
        """
        s3 = S3Hook(aws_conn_id=MINIO_CONN_ID)
        bucket = MinioConfig.BUCKET_RAW_DATA
        prefix = MinioConfig.KAGGLE_PREFIX

        keys = s3.list_keys(bucket_name=bucket, prefix=prefix)

        first_bloom_parts = []
        full_bloom_parts = []

        for key in keys:
            content = s3.read_key(key, bucket_name=bucket)
            df = pd.read_csv(StringIO(content))

            # 1. Identify Year columns vs Metadata columns
            year_cols = [c for c in df.columns if c.isdigit()]
            # Everything else is metadata (Site Name, Notes, etc.)
            meta_cols = [c for c in df.columns if not c.isdigit()]

            # 2. Melt: Keep all meta_cols as identifiers, turn years into rows
            melted = df.melt(
                id_vars=meta_cols,
                value_vars=year_cols,
                var_name='year',
                value_name='bloom_timestamp'
            )

            if 'first_bloom' in key:
                first_bloom_parts.append(melted)
            elif 'full_bloom' in key:
                full_bloom_parts.append(melted)

        df_first = pd.concat(first_bloom_parts).rename(columns={'bloom_timestamp': 'first_bloom_raw'})
        df_full = pd.concat(full_bloom_parts).rename(columns={'bloom_timestamp': 'full_bloom_raw'})

        # Join the two dataframes
        # We merge primarily on Site Name and year
        merged_df = pd.merge(df_first, df_full, on=['Site Name', 'year'], how='outer')

        # Group by Site Name and year to combine rows that should be one
        merged_df = merged_df.groupby(['Site Name', 'year'], as_index=False).agg({
            'Currently Being Observed_x': 'first',
            '30 Year Average 1991-2020_x': 'first',
            'Notes_x': 'first',
            'first_bloom_raw': 'first',
            'full_bloom_raw': 'first'
        })

        # Rename columns back to original names after groupby
        merged_df = merged_df.rename(columns={
            'Currently Being Observed_x': 'Currently Being Observed',
            '30 Year Average 1991-2020_x': '30 Year Average 1991-2020',
            'Notes_x': 'Notes'
        })

        # 4. Clean up: Remove rows where both dates are empty
        merged_df = merged_df.dropna(subset=['first_bloom_raw', 'full_bloom_raw'], how='all')
        # 5. Data Transformations
        # Generate UUID for each location
        merged_df['location_id'] = merged_df['Site Name'].apply(
            lambda x: str(uuid.uuid5(uuid.NAMESPACE_DNS, str(x)))
        )

        # Function to clean timestamp strings into DATE objects
        def clean_date(val):
            if pd.isna(val) or str(val).strip() == "" or str(val).strip() == "nan":
                return None
            try:
                return pd.to_datetime(val).strftime('%Y-%m-%d')
            except:
                return None

        merged_df['first_bloom_date'] = merged_df['first_bloom_raw'].apply(clean_date)
        merged_df['full_bloom_date'] = merged_df['full_bloom_raw'].apply(clean_date)

        # 6. Final Selection (match Snowflake columns)
        final_df = merged_df[[
            'location_id',
            'Site Name',
            'Currently Being Observed',
            '30 Year Average 1991-2020',
            'Notes',
            'year',
            'first_bloom_date',
            'full_bloom_date'
        ]]

        temp_path = "/tmp/transformed_sakura_full.csv"
        final_df.to_csv(temp_path, index=False, header=False, sep='|')

        logger.info(f"Transformation complete. Total records: {len(final_df)}")
        return temp_path

    @task
    def upload_to_snowflake(file_path: str):
        """Load into Snowflake using the Stage."""
        sn_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

        # PUT local file to internal stage
        put_sql = f"PUT file://{file_path} @SAKURA_DB.RAW.KAGGLE_STAGE auto_compress=true"
        sn_hook.run(put_sql)

        # COPY INTO table.
        copy_sql = """
            COPY INTO SAKURA_DB.RAW.SAKURA_BLOOM_TRANSPOSED (
                location_id, site_name, currently_observed, average_30_year, 
                notes, year, first_bloom_date, full_bloom_date
            )
            FROM @SAKURA_DB.RAW.KAGGLE_STAGE
            FILE_FORMAT = (
                TYPE = 'CSV' 
                FIELD_DELIMITER = '|'
                FIELD_OPTIONALLY_ENCLOSED_BY = '"' 
                NULL_IF = ('', 'None', 'nan')
            )
            PURGE = TRUE;
        """
        sn_hook.run(copy_sql)

        if os.path.exists(file_path):
            os.remove(file_path)
        logger.info("Successfully loaded full dataset to Snowflake.")

    # Task flow
    file_to_load = process_and_transform()
    upload_to_snowflake(file_to_load)

sakura_full_transposition_pipeline()