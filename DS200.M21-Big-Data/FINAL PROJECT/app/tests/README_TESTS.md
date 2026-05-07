# Testing Guide

## Full Pipeline Runbook

### 0) Go to project root
```bash
cd "/home/thinh/Documents/IS_BigData/DS200.M21-Big-Data/FINAL PROJECT"
```

### 1) Set environment
```bash
export SPARK_HOME=/home/thinh/spark
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3
export PYTHONPATH="$PWD:${PYTHONPATH}"
export SPARK_LOCAL_DIRS=/home/thinh/spark_local
export SPARK_KAFKA_PACKAGES="org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1"
```

### 2) Train on the full Kaggle dataset
```bash
bash scripts/run_spark_v2_fixed.sh "kaggle_youtube/trending_yt_videos_113_countries.csv" \
  --no-sample \
  20 \
  8 \
  /home/thinh/rf_model_v2_fixed
```

### 3) Check training outputs
```bash
ls -lh /home/thinh/rf_model_v2_fixed
cat Images/metrics_v2_fixed.json
```

### 4) Start Kafka services
Open two terminals and run:
```bash
~/kafka/bin/zookeeper-server-start.sh ~/kafka/config/zookeeper.properties
```

```bash
~/kafka/bin/kafka-server-start.sh ~/kafka/config/server.properties
```

### 5) Create Kafka topics
```bash
~/kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --topic youtube_videos --partitions 1 --replication-factor 1
~/kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --topic youtube_predictions --partitions 1 --replication-factor 1
```

### 6) Start the streaming prediction job
```bash
python3 -m app.streaming_spark \
  --kafka-servers localhost:9092 \
  --input-topic youtube_videos \
  --output-topic youtube_predictions \
  --model-path /home/thinh/rf_model_v2_fixed \
  --output kafka
```

### 7) Start the producer
```bash
python3 -m app.producer_youtube \
  --kafka-servers localhost:9092 \
  --topic youtube_videos \
  --source synthetic \
  --rate 1 \
  --num-messages 5
```

### 8) Optional: watch prediction messages
```bash
python3 -m app.consumer_predictions \
  --kafka-servers localhost:9092 \
  --topic youtube_predictions
```

## Output Locations

- Model: `/home/thinh/rf_model_v2_fixed`
- Training metrics: `Images/metrics_v2_fixed.json`
- Streaming output topic: `youtube_predictions`
- Streaming console output: the terminal running `app.streaming_spark`
- Consumer display: the terminal running `app.consumer_predictions`

## Running Unit Tests

### Prerequisites
```bash
pip install pytest pyspark matplotlib
```

### Run All Tests
```bash
cd "/home/thinh/Documents/IS_BigData/DS200.M21-Big-Data/FINAL PROJECT"
bash scripts/test_v2_fixed.sh
```

### Quick Validation
```bash
bash scripts/test_v2_fixed.sh
```

---

## What Each Test Module Tests

### V2 Fixed Validation
- `scripts/test_v2_fixed.sh` - Quick end-to-end leakage regression check
- `app/app_spark_v2_fixed.py` - Full training entry point
- `app/clean_spark_v2_fixed.py` - Leakage-free feature engineering
- `app/train_spark_v2_fixed.py` - Leakage-free model training

---

## Running Full Pipeline with Visualization

### Full Train (local full dataset)
```bash
bash scripts/run_spark_v2_fixed.sh "kaggle_youtube/trending_yt_videos_113_countries.csv" --no-sample 20 8 /home/thinh/rf_model_v2_fixed
```

### Outputs
After running, check:
- **Model metrics:** `Images/metrics_v2_fixed.json`
- **Model:** `/home/thinh/rf_model_v2_fixed`
- **Streaming output:** console or Kafka topic `youtube_predictions`

---

## Testing Kafka Streaming (Future)

### Preview
```bash
python -m app.streaming_spark --help
```

**Note:** Full Kafka streaming requires:
- Kafka broker running on `localhost:9092`
- Pre-trained model saved locally at `/home/thinh/rf_model_v2_fixed`
- Input topic with properly formatted JSON messages

See `app/streaming_spark.py` for integration details.

---

## CI/CD Integration

### Example GitHub Actions Workflow
```yaml
name: Tests
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - uses: actions/setup-python@v2
        with:
          python-version: '3.9'
      - run: pip install -r requirements.txt pytest
      - run: bash scripts/test_v2_fixed.sh
```

---

## Troubleshooting

**Issue:** `No module named 'pyspark'`
```bash
pip install pyspark
```

**Issue:** `No module named 'matplotlib'`
```bash
pip install matplotlib
```

**Issue:** Tests run but visualizations fail
- Visualization failures are non-critical
- Check if `Images/` directory exists: `mkdir -p Images`
- Run with `--no-visualizations` flag if issues persist

---
