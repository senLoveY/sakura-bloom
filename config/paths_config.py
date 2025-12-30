class MinioConfig:
    
    BUCKET_RAW_DATA = "sakura-bloom-data"
    BUCKET_GEO_DATA = "geo-data"
    BUCKET_WEATHER_DATA = "weather-data"

    RAW_BLOOM_DATA_FILE = "kaggle/sakura_full_bloom_dates.csv"

    GEO_NOMINATIM_PATH = "nominatim/{city}/{timestamp}.json"
