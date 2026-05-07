# DS200.M21 - Big Data


DS200.M21-Phân Tích Dữ Liệu Lớn

Học kỳ 2 Năm 3 Năm học 2021-2022 

## Final Project - Hệ Thống Dự Đoán Độ Trễ Chuyến Bay Theo Thời Gian Thực

**Giảng Viên:** 
- TS Đỗ Trọng Hợp


**Nhóm SVTH:**
# DS200.M21 - YouTube Trending Prediction (Final Project)

Project repository for the course DS200.M21. This project implements an end-to-end pipeline to predict whether a YouTube video will be "Trending" using Spark ML for training and Spark Structured Streaming + Kafka for realtime prediction.

**Lecturer:**
- TS Đỗ Trọng Hợp

**Team Members:**
- Phạm Đức Thể
- Võ Minh Trí
- Trần Triệu Vũ

## Summary

This repository contains code to train a Random Forest classifier (batch) and a streaming application that consumes video events from Kafka, applies the trained model, and publishes predictions back to Kafka. The implementation includes utilities to produce synthetic video events and a simple consumer to display realtime prediction results.

Key components (in `FINAL PROJECT`):
- `app/app_spark_v2_fixed.py` — batch training entrypoint (v2, fixed leakage)
- `app/streaming_spark.py` — Spark Structured Streaming job (reads `youtube_videos`, writes `youtube_predictions`)
- `app/producer_youtube.py` — Kafka producer (synthetic or real CSV source)
- `app/consumer_predictions.py` — small consumer to print predictions
- `scripts/run_spark_v2_fixed.sh` — training wrapper that sets Spark env and runs the trainer

## Dataset

The training dataset used for experiments is placed under `FINAL PROJECT/kaggle_youtube/` (csv of trending videos across countries). The training script aggregates and prepares features before training.

## Quickstart — copy/paste to run end-to-end

1) Train model (saves model to `/home/thinh/rf_model_v2_fixed` by default)

```bash
cd "FINAL PROJECT"
bash scripts/run_spark_v2_fixed.sh
```

2) Start Zookeeper and Kafka (in separate terminals)

```bash
cd ~/kafka
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties
```

3) Create topics (if they don't exist)

```bash
cd ~/kafka
bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --topic youtube_videos --partitions 1 --replication-factor 1
bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --topic youtube_predictions --partitions 1 --replication-factor 1
```

4) Start streaming prediction job (wait until it prints "Writing predictions to Kafka topic")

```bash
cd "FINAL PROJECT"
export SPARK_HOME=/home/thinh/spark
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PYTHONPATH="$PWD:${PYTHONPATH}"
export SPARK_KAFKA_PACKAGES="org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1"
python3 -m app.streaming_spark --kafka-servers localhost:9092 --input-topic youtube_videos --output-topic youtube_predictions --model-path /home/thinh/rf_model_v2_fixed --output kafka
```

5) Start consumer to view realtime predictions

```bash
cd "FINAL PROJECT"
export PYTHONPATH="$PWD:${PYTHONPATH}"
python3 -m app.consumer_predictions --kafka-servers localhost:9092 --topic youtube_predictions
```

6) Produce test messages (synthetic) — use `--burst-size` and `--non-trending-ratio` to control variety

```bash
cd "FINAL PROJECT"
export PYTHONPATH="$PWD:${PYTHONPATH}"
python3 -m app.producer_youtube --kafka-servers localhost:9092 --topic youtube_videos --source synthetic --rate 2 --burst-size 10 --non-trending-ratio 0.5 --num-messages 100
```

7) (Optional) Read raw JSON predictions with Kafka console consumer

```bash
~/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic youtube_predictions --from-beginning --max-messages 20
```

## Notes & Tips
- Start the streaming job before the producer so the sink is attached and predictions are emitted live. The consumer in `app/consumer_predictions.py` uses `auto_offset_reset='latest'` by default.
- Training logs and metrics are saved under `FINAL PROJECT/Images/` and model is saved to the path printed by the training script.
- If Spark modules are not found, ensure `SPARK_HOME` is exported and `PYTHONPATH` includes the project root. See `scripts/run_spark_v2_fixed.sh` for example env setup.

## Next steps you might want
- Save a short `QUICKSTART.md` with these commands (I can add it for you).
- Generate evaluation plots from `Images/metrics_v2_fixed.json` (not done yet).

---
Updated to reflect the YouTube Trending prediction final project.
