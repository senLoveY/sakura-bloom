class MinioConfig:

    # Названия бакетов (созданные в DAG 01)
    BUCKET_RAW_DATA = "sakura-bloom-data"
    BUCKET_GEO_DATA = "geo-data"
    BUCKET_WEATHER_DATA = "weather-data"

    # Пути к исходным файлам
    # Путь внутри бакета BUCKET_RAW_DATA
    RAW_BLOOM_DATA_FILE = "kaggle/sakura_full_bloom_dates.csv"

    # Шаблоны путей для сохранения (Output paths)
    # Используем .format() или f-strings при вызове
    GEO_NOMINATIM_PATH = "nominatim/{city}/{timestamp}.json"