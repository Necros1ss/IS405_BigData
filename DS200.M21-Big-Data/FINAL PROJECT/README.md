# YouTube Trending Prediction — FINAL PROJECT

Short README to reproduce core pipeline quickly and reliably (suitable for CV viewers).

Prereqs
- Python 3.8+
- Java 17, Spark (if running Spark jobs locally)
- Optional: Kafka for streaming

Quick run (batch ETL + train)

```bash
cd "/home/thinh/Documents/IS_BigData/DS200.M21-Big-Data/FINAL PROJECT"
# Activate venv if you have one
source .venv_spark/bin/activate  # optional

# 1) Run cleaning (reads CSV(s) from data/ and writes parquet)
python3 -m app.spark_data_cleaner

# 2) Train model (reads cleaned parquet)
python3 -m app.app_spark --data "data/cleaned_youtube_regression.parquet" --num-trees 20 --max-depth 6 --save-model "models/rf_model_demo"
```

Quick streaming (requires Kafka broker)

```bash
# Start streaming processor
python3 -m app.streaming_spark --kafka-servers localhost:9092 --input-topic youtube_videos --output-topic youtube_predictions --model-path models/rf_model_demo --checkpoint-dir /tmp/spark_chkpt_youtube

# Start producer against the real YouTube Trending feed (requires YOUTUBE_API_KEY)
python3 -m app.producer_youtube --kafka-servers localhost:9092 --topic youtube_videos --rate 1 --max-results 5

# Start producer with real YouTube API (requires YOUTUBE_API_KEY)
# python3 -m app.producer_youtube --kafka-servers localhost:9092 --topic youtube_videos --region-code US --max-results 5 --poll-interval 60

# Start consumer (another terminal)
python3 -m app.consumer_predictions --kafka-servers localhost:9092 --topic youtube_predictions
```

Notes
- If Kafka fails with `InconsistentClusterIdException`, run `bash scripts/reset_kafka.sh` before starting streaming.
- If you plan to run Spark locally, ensure `JAVA_HOME` and `SPARK_HOME` are set and add Spark Python paths to `PYTHONPATH`.

This repository is intentionally kept minimal for final submission/demo: core app code, required scripts, and concise run guides.
