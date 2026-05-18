import os
try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

if load_dotenv is not None:
	load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
YOUTUBE_TOPIC = os.getenv("YOUTUBE_TOPIC", "youtube_videos")
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY", "")

RAW_DATA_PATH = os.getenv("RAW_DATA_PATH", "data/GLOBAL_youtube_trending.csv")
CLEAN_DATA_PATH = os.getenv("CLEAN_DATA_PATH", "data/cleaned_youtube_regression.parquet")

SPARK_APP_NAME = os.getenv("SPARK_APP_NAME", "YouTubeTrendingDataCleaner")