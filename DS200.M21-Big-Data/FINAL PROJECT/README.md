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
python3 -m app.app_spark --data "data/cleaned_youtube_regression.parquet" --num-trees 20 --max-depth 6 --save-model "models/rf_regression_model"
```

Quick streaming demo (requires Kafka broker)

```bash
# 0) Clean Kafka state
bash scripts/reset_kafka.sh

# 1) Recreate the topics used by the stream
cd "$HOME/kafka"
./bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic youtube_videos --partitions 1 --replication-factor 1
./bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic youtube_predictions --partitions 1 --replication-factor 1

# 2) Start the streaming predictor (new checkpoint when Kafka topics are recreated)
cd "/home/thinh/Documents/IS_BigData/DS200.M21-Big-Data/FINAL PROJECT"
python3 -m app.streaming_spark --kafka-servers localhost:9092 --input-topic youtube_videos --output-topic youtube_predictions --model-path models/rf_regression_model --checkpoint-dir /tmp/spark_chkpt_youtube_2

# 3) Start the consumer in another terminal
python3 -m app.consumer_predictions --kafka-servers localhost:9092 --topic youtube_predictions

# 4) Send one producer batch and exit after 20 messages
python3 -m app.producer_youtube --kafka-servers localhost:9092 --topic youtube_videos --region-code US --max-results 20 --poll-interval 1 --num-messages 20
```

If you want continuous batches instead of a single demo batch, use:

```bash
cd "/home/thinh/Documents/IS_BigData/DS200.M21-Big-Data/FINAL PROJECT"

bash run_producer_loop.sh
```

Notes
- If Kafka fails with `InconsistentClusterIdException`, run `bash scripts/reset_kafka.sh` before starting streaming.
- If you recreate Kafka topics, use a fresh Spark checkpoint directory like `/tmp/spark_chkpt_youtube_2` so the streaming query does not reuse stale offsets.
- `run_producer_loop.sh` loads `.env` automatically, so the YouTube API key can stay in the project file.
- If you plan to run Spark locally, ensure `JAVA_HOME` and `SPARK_HOME` are set and add Spark Python paths to `PYTHONPATH`.

This repository is intentionally kept minimal for final submission/demo: core app code, required scripts, and concise run guides.
