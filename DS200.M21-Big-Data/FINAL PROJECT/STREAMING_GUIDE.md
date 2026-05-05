# Hướng dẫn Kafka Streaming YouTube Trending Prediction

## 🎯 Tổng quan

Pipeline streaming cho phép dự đoán realtime các video YouTube sẽ trending hay không, khi video được upload liên tục.

**Kiến trúc:**
```
Producer (Gửi video mới)
    ↓
Kafka Topic: youtube_videos
    ↓
Spark Streaming (Feature Engineering + ML Predictions)
    ↓
Kafka Topic: youtube_predictions
    ↓
Consumer (Xem kết quả)
```

---

## 📋 Yêu cầu chuẩn bị

### 1. Cài Kafka (chọn 1 trong 2 cách)

#### **Cách A: Cài Kafka local (khuyên dùng)**
```bash
cd ~
wget https://archive.apache.org/dist/kafka/3.6.0/kafka_2.13-3.6.0.tgz
tar -xzf kafka_2.13-3.6.0.tgz
mv kafka_2.13-3.6.0 ~/kafka
```

#### **Cách B: Dùng Docker** (nếu có Docker)
```bash
# Start ZooKeeper + Kafka
docker-compose up -d

# Với docker-compose.yml:
version: '3'
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
  
  kafka:
    image: confluentinc/cp-kafka:7.5.0
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
```

### 2. Cài Python dependencies
```bash
pip3 install kafka-python==2.0.2
```

### 3. Train model trước (nếu muốn predictions)
```bash
cd "/home/thinh/Documents/IS_BigData/DS200.M21-Big-Data/FINAL PROJECT"
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export SPARK_HOME=/home/thinh/spark
export PYTHONPATH="$PWD:$SPARK_HOME/python:$SPARK_HOME/python/lib/pyspark.zip"

python3 -m app.app_spark --data app/data_final/youtube_large_sample.csv \
  --no-sample --save-model /tmp/rf_model
```

---

## 🚀 Chạy Streaming Pipeline

### **Terminal 1: Start ZooKeeper**
```bash
cd ~/kafka
bin/zookeeper-server-start.sh config/zookeeper.properties
```

### **Terminal 2: Start Kafka Broker**
```bash
cd ~/kafka
bin/kafka-server-start.sh config/server.properties
```

### **Terminal 3: Create Topics**
```bash
cd ~/kafka

# Topic nhận dữ liệu từ producer
bin/kafka-topics.sh --create --bootstrap-server localhost:9092 \
  --topic youtube_videos --partitions 1 --replication-factor 1

# Topic output predictions
bin/kafka-topics.sh --create --bootstrap-server localhost:9092 \
  --topic youtube_predictions --partitions 1 --replication-factor 1
```

### **Terminal 4: Start Spark Streaming Pipeline**
```bash
cd "/home/thinh/Documents/IS_BigData/DS200.M21-Big-Data/FINAL PROJECT"
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export SPARK_HOME=/home/thinh/spark
export PYTHONPATH="$PWD:$SPARK_HOME/python:$SPARK_HOME/python/lib/pyspark.zip"

# Test mode (output to console, không cần model)
python3 -m app.streaming_spark \
  --kafka-servers localhost:9092 \
  --input-topic youtube_videos \
  --output console

# Hoặc với model (predictions thực)
python3 -m app.streaming_spark \
  --kafka-servers localhost:9092 \
  --input-topic youtube_videos \
  --model-path /tmp/rf_model \
  --output console
```

### **Terminal 5: Start Producer (Gửi video)**
```bash
cd "/home/thinh/Documents/IS_BigData/DS200.M21-Big-Data/FINAL PROJECT"
export PYTHONPATH="$PWD"

# Gửi 1 video mỗi giây
python3 -m app.producer_youtube \
  --kafka-servers localhost:9092 \
  --topic youtube_videos \
  --rate 1

# Hoặc gửi nhanh hơn (2 videos/giây)
python3 -m app.producer_youtube \
  --kafka-servers localhost:9092 \
  --topic youtube_videos \
  --rate 2
```

### **Terminal 6 (optional): Monitor Predictions**
```bash
cd "/home/thinh/Documents/IS_BigData/DS200.M21-Big-Data/FINAL PROJECT"
export PYTHONPATH="$PWD"

python3 -m app.consumer_predictions \
  --kafka-servers localhost:9092 \
  --topic youtube_predictions
```

---

## 📊 Các modes chạy

### Mode 1: Test (Console Output)
```bash
# Không cần model, xem realtime JSON từ streaming
python3 -m app.streaming_spark --output console
```

**Output:**
```
[10:15:32] Processing video vid_123456
[10:15:32] Features: {tag_count: 3, description_length: 215, ...}
[10:15:32] Prediction: 1 (Trending), prob: [0.15, 0.85]
```

### Mode 2: Production (Kafka Output)
```bash
# Output predictions vào Kafka topic youtube_predictions
python3 -m app.streaming_spark \
  --model-path /tmp/rf_model \
  --output kafka
```

---

## 🔧 Tuning Parameters

| Parameter | Giá trị mặc định | Ý nghĩa |
|-----------|-----------------|---------|
| `--kafka-servers` | `localhost:9092` | Kafka broker address |
| `--input-topic` | `youtube_videos` | Topic nhận video từ producer |
| `--output-topic` | `youtube_predictions` | Topic ghi predictions |
| `--model-path` | None | Path tới model (không bắt buộc) |
| `--checkpoint-dir` | `/tmp/spark_checkpoint` | Fault tolerance checkpoint |
| `--output` | `console` | `console` hoặc `kafka` |

---

## 🐛 Troubleshooting

| Vấn đề | Nguyên nhân | Giải pháp |
|--------|-----------|----------|
| `Connection refused` | Kafka chưa chạy | Start Kafka broker |
| `Topic not found` | Topic chưa được tạo | Chạy `kafka-topics.sh --create ...` |
| `No model loaded` | Model path sai | Kiểm tra `--model-path` argument |
| `Out of memory` | Spark consumer quá nhiều data | Giảm `--rate` producer |
| `Predictions all 0` | Model chưa train | Sửa `--model-path` hoặc train model |

---

## 📝 Ví dụ thực tế

### Scenario 1: Demo with Console Output
```bash
# Terminal 1-3: Setup Kafka (như trên)

# Terminal 4: Streaming
python3 -m app.streaming_spark --output console

# Terminal 5: Producer (send 1 video every 2 seconds)
python3 -m app.producer_youtube --rate 0.5

# Xem output realtime trong Terminal 4
```

### Scenario 2: Full Production Setup
```bash
# Terminal 1-3: Setup Kafka

# Terminal 4: Streaming with Model
python3 -m app.streaming_spark \
  --model-path /tmp/rf_model \
  --output kafka

# Terminal 5: Producer
python3 -m app.producer_youtube --rate 1

# Terminal 6: Consumer
python3 -m app.consumer_predictions

# Xem predictions realtime trong Terminal 6
```

### Scenario 3: High Volume Test
```bash
# Producer: 10 videos/second
python3 -m app.producer_youtube --rate 10

# Streaming sẽ process micro-batches realtime
```

---

## 📈 Performance Benchmarks

| Metrics | Value |
|---------|-------|
| Latency (processing) | ~500ms per batch |
| Throughput | 1-100 videos/second |
| Memory (Spark) | ~500MB base |
| Memory (Kafka) | ~200MB |
| CPU (Spark) | 1-2 cores |

---

## 🎓 Tiếp theo

1. **Lưu predictions vào database** → Thêm Parquet/Delta writer
2. **Alerting** → Gửi email khi video trending
3. **Model retraining** → Cập nhật model định kỳ
4. **Scale up** → Dùng HDFS/S3 cho checkpoint
5. **Visualizations** → Dashboard realtime với Grafana

---

## 📞 Hỗ trợ

**Cần debug?**
```bash
# Xem Kafka logs
tail -f ~/kafka/logs/server.log

# Xem Spark logs
grep "ERROR" /tmp/spark_logs/*.log

# Liệt kê Kafka topics
~/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092

# Delete topic (nếu cần reset)
~/kafka/bin/kafka-topics.sh --delete --bootstrap-server localhost:9092 --topic youtube_videos
```

Happy streaming! 🎉
