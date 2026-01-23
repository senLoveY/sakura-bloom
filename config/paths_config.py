class MinioConfig:
    BUCKET_RAW_DATA = "sakura-bloom-data"
    BUCKET_GEO_DATA = "geo-data"
    BUCKET_WEATHER_DATA = "weather-data"

    KAGGLE_PREFIX = "kaggle"

    RAW_BLOOM_DATA_FILE = f"{KAGGLE_PREFIX}/full_bloom_2010_2025.csv"
    DATASET_URI = f"s3://{BUCKET_RAW_DATA}/{KAGGLE_PREFIX}"
    GEO_NOMINATIM_PATH = "nominatim/{city}/{timestamp}.json"
    
    
    VISUALCROSSING_PREFIX = "visualcrossing"
    WEATHER_DATA_PATH = f"{VISUALCROSSING_PREFIX}/{{city}}/{{timestamp}}.json"

    
    
class VisualCrossingConfig:

    BASE_URL = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline"

    UNIT_GROUP = "metric" 
    CONTENT_TYPE = "json"
    INCLUDE = "days"  
    

    ELEMENTS = "datetime,tempmax,tempmin,temp"  

    START_DATE = "2025-01-01"
    END_DATE = "2025-01-07"
    

    DELAY_BETWEEN_REQUESTS = 0.5  
    

    COUNTRY = "Japan" 