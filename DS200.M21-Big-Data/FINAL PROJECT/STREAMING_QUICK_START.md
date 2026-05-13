# Streaming Quick Reference

Quick commands that match current code in app/.

## 1) Prepare model

```bash
cd "/home/thinh/Documents/IS_BigData/DS200.M21-Big-Data/FINAL PROJECT"

# Optional venv
source .venv_spark/bin/activate

# ETL + train
python3 -m app.spark_data_cleaner
python3 -m app.app_spark_v2_fixed \
  --data "data/cleaned_youtube_regression.parquet" \
  --num-trees 20 \
  --max-depth 6 \
  --save-model "models/rf_model_demo"
```

## 2) Start Kafka infra (separate terminals)

```bash
# Terminal 1
~/kafka/bin/zookeeper-server-start.sh ~/kafka/config/zookeeper.properties

# Terminal 2
~/kafka/bin/kafka-server-start.sh ~/kafka/config/server.properties

# Terminal 3 (create topics once)
~/kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --topic youtube_videos --partitions 1 --replication-factor 1
~/kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --topic youtube_predictions --partitions 1 --replication-factor 1
```

## 3) Run streaming prediction

```bash
# Terminal 4: Spark streaming processor
python3 -m app.streaming_spark \
  --kafka-servers localhost:9092 \
  --input-topic youtube_videos \
  --output-topic youtube_predictions \
  --model-path models/rf_model_demo \
  --checkpoint-dir /tmp/spark_chkpt_youtube

# Terminal 5: producer (synthetic demo mode, no API key needed)
python3 -m app.producer_youtube \
  --kafka-servers localhost:9092 \
  --topic youtube_videos \
  --source synthetic \
  --rate 1 \
  --max-results 5

# Terminal 6: prediction consumer
python3 -m app.consumer_predictions \
  --kafka-servers localhost:9092 \
  --topic youtube_predictions
```

## Real API mode

```bash
export YOUTUBE_API_KEY="your_key"
python3 -m app.producer_youtube \
  --kafka-servers localhost:9092 \
  --topic youtube_videos \
  --source api \
  --region-code US \
  --max-results 10 \
  --poll-interval 60
```

## Common issues

```bash
# Kafka connection refused
# -> Ensure broker is running on localhost:9092

# Model path not found
# -> Re-run training and verify models/rf_model_demo exists

# Missing PySpark
# -> pip install -r requirements.txt
```
