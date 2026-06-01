import os
from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

if load_dotenv is not None:
    project_root_env = Path(__file__).resolve().parents[1] / ".env"
    load_dotenv(dotenv_path=project_root_env)
    load_dotenv()
else:
    project_root_env = Path(__file__).resolve().parents[1] / ".env"
    if project_root_env.is_file():
        with project_root_env.open("r", encoding="utf-8") as handle:
            for line in handle:
                stripped = line.strip()
                if not stripped or stripped.startswith("#"):
                    continue
                if stripped.startswith("export "):
                    stripped = stripped[len("export ") :]
                if "=" not in stripped:
                    continue
                key, value = stripped.split("=", 1)
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                if key and key not in os.environ:
                    os.environ[key] = value

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
YOUTUBE_TOPIC = os.getenv("YOUTUBE_TOPIC", "youtube_videos")
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY", "")

RAW_DATA_PATH = os.getenv("RAW_DATA_PATH", "data/GLOBAL_youtube_trending.csv")
CLEAN_DATA_PATH = os.getenv("CLEAN_DATA_PATH", "data/cleaned_youtube_regression.parquet")

SPARK_APP_NAME = os.getenv("SPARK_APP_NAME", "YouTubeTrendingDataCleaner")