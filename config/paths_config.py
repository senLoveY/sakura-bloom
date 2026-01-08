class MinioConfig:
    BUCKET_RAW_DATA = "sakura-bloom-data"
    BUCKET_GEO_DATA = "geo-data"
    BUCKET_WEATHER_DATA = "weather-data"

    KAGGLE_PREFIX = "kaggle"

    RAW_BLOOM_DATA_FILE = f"{KAGGLE_PREFIX}/full_bloom_2010_2025.csv"
    DATASET_URI = f"s3://{BUCKET_RAW_DATA}/{KAGGLE_PREFIX}"
    GEO_NOMINATIM_PATH = "nominatim/{city}/{timestamp}.json"