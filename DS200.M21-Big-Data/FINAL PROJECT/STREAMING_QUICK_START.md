# ⚡ Streaming Quick Reference

## 📦 Những gì vừa được tạo

| File | Mô tả | Dòng code |
|------|-------|----------|
| `app/streaming_spark.py` | Spark Structured Streaming + Kafka inference | 350+ |
| `app/producer_youtube.py` | Gửi synthetic video data → Kafka | 120+ |
| `app/consumer_predictions.py` | Xem predictions realtime | 100+ |
| `app/app_spark.py` | Batch training + model export | 220+ |
| `STREAMING_GUIDE.md` | Hướng dẫn chi tiết | 400+ |
| `test_streaming_setup.py` | Kiểm tra hệ thống sẵn sàng | 150+ |

**Total:** 1400+ dòng code mới cho streaming! 🚀

---

## 🎯 Cách dùng (3 bước)

### Bước 1: Cài Kafka
```bash
cd ~
wget https://archive.apache.org/dist/kafka/3.6.0/kafka_2.13-3.6.0.tgz
tar -xzf kafka_2.13-3.6.0.tgz
mv kafka_2.13-3.6.0 ~/kafka
```

### Bước 2: Kiểm tra setup
```bash
cd "/path/to/FINAL PROJECT"
python3 test_streaming_setup.py  # ✅ Sẽ báo everything is ready
```

Nếu dùng Spark streaming với Kafka connector, nhớ đặt thêm:
```bash
export SPARK_KAFKA_PACKAGES="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
```

### Bước 3: Chạy streaming (6 terminals)

| # | Terminal | Command |
|---|----------|---------|
| 1 | `t1` | `~/kafka/bin/zookeeper-server-start.sh config/zookeeper.properties` |
| 2 | `t2` | `~/kafka/bin/kafka-server-start.sh config/server.properties` |
| 3 | `t3` | `~/kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --topic youtube_videos --partitions 1` |
| 4 | `t4` | `python3 -m app.streaming_spark --output console` |
| 5 | `t5` | `python3 -m app.producer_youtube --rate 1` |
| 6 | `t6` | `python3 -m app.consumer_predictions` |

---

## 📊 Streaming Input/Output

### **Input** (từ Producer)
```json
{
  "video_id": "vid_123",
  "event_time": "2026-05-05T10:15:30Z",
  "title": "Amazing Video",
  "view_count": 500000,
  "like_count": 40000,
  "comment_count": 2000,
  "video_tags": "trending|viral",
  "description": "..."
}
```
↓ **Feature Engineering** ↓

### **Output** (từ Streaming)
```json
{
  "video_id": "vid_123",
  "event_time": "2026-05-05T10:15:30Z",
  "title": "Amazing Video",
  "tag_count": 2,
  "description_length": 21,
  "like_ratio": 0.08,
  "comment_ratio": 0.004,
  "engagement": 42000,
  "trending": 1,           ← Dự đoán (1=trending, 0=not)
  "prob_trending": 0.85    ← Độ tin cậy
}
```

---

## 🔧 Advanced Commands

```bash
# Test console output (không cần model)
python3 -m app.streaming_spark --output console

# Với pre-trained model
python3 -m app.streaming_spark \
  --output console \
  --model-path /tmp/rf_model

# Train model batch trước khi chạy realtime
python3 -m app.app_spark --data <path-to-csv> --no-sample --save-model /tmp/rf_model

# Production mode (output → Kafka topic)
python3 -m app.streaming_spark \
  --output kafka \
  --model-path /tmp/rf_model \
  --output-topic youtube_predictions

# Producer: 2 videos/second
python3 -m app.producer_youtube --rate 2

# Producer: 1 video every 10 seconds
python3 -m app.producer_youtube --rate 0.1

# Consumer: Monitor predictions
python3 -m app.consumer_predictions --topic youtube_predictions
```

---

## ❓ FAQ

**Q: Cần model không?**  
A: Không bắt buộc để chạy parse/feature, nhưng để có prediction thì cần `--model-path` trỏ tới model đã train batch.

**Q: Làm sao train model?**  
A: Chạy batch pipeline trước:
```bash
python3 -m app.app_spark --data youtube_large_sample.csv \
  --no-sample --save-model /tmp/rf_model
```

**Q: Có thể gửi data thực không?**  
A: Có. Producer hiện sinh dữ liệu mô phỏng, nhưng bạn có thể thay `create_synthetic_video()` bằng API YouTube hoặc service thu thập log thật.

**Q: Latency như thế nào?**  
A: Là micro-batch streaming, độ trễ phụ thuộc trigger interval, Kafka, và Spark cluster.

**Q: Scale tới bao nhiêu videos/second?**  
A: Có thể handle 1-100 videos/second. Tùy tài nguyên server.

---

## 🐛 Nếu có lỗi

```bash
# Error: Connection refused
→ Kafka chưa chạy. Check lại terminal 1 & 2.

# Error: Topic not found
→ Chạy kafka-topics.sh --create (xem terminal 3 ở trên)

# Error: Model not found
→ Kiểm tra đường dẫn model hoặc bỏ --model-path

# Error: Out of memory
→ Giảm --rate producer hoặc --num-messages
```

---

## 📖 Tài liệu đầy đủ

Xem: **STREAMING_GUIDE.md**

---

## 🎉 Ready to stream! 

```bash
# Setup check
python3 test_streaming_setup.py

# Start!
# (6 terminals như trên)
```

**Happy streaming!** 🚀
