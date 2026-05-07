# 🎯 STREAMING EXECUTION GUIDE - With Troubleshooting

**Status:** ETL ✅ + Training ✅ + Kafka Setup with Cluster ID Fix

---

## 🔧 Known Kafka Issue & Fix

**Error:** `InconsistentClusterIdException`
- Cause: Stale Kafka metadata from previous runs  
- Solution: Clean Kafka storage before starting

### Quick Fix - Reset Kafka

```bash
# 1. Stop all Kafka/Zookeeper processes
pkill -f "kafka\|zookeeper"
sleep 2

# 2. Clean Kafka storage
rm -rf ~/kafka/data/* /tmp/kafka-logs* /tmp/zookeeper*

# 3. Restart Zookeeper (Terminal 1)
cd ~/kafka
bin/zookeeper-server-start.sh config/zookeeper.properties

# 4. Wait 5 seconds, then restart Kafka (Terminal 2)
sleep 5
cd ~/kafka
bin/kafka-server-start.sh config/server.properties

# 5. Wait 10 seconds for broker to initialize
sleep 10
echo "✓ Broker ready"
```

---

## 📡 Complete Streaming Pipeline (6 Terminals)

**Prerequisite:** Kafka must be running and healthy after reset above.

### Terminal 1: Verify Kafka is Ready

```bash
cd ~/kafka
bin/kafka-topics.sh --list --bootstrap-server localhost:9092

# Expected output:
# __consumer_offsets
# (empty if no topics created yet)
```

### Terminal 2: Create Topics

```bash
cd ~/kafka

bin/kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --topic youtube_videos \
  --partitions 1 --replication-factor 1 --if-not-exists

bin/kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --topic youtube_predictions \
  --partitions 1 --replication-factor 1 --if-not-exists

echo "✓ Topics created"
```

### Terminal 3: Start Producer (Generate Synthetic Data)

```bash
cd "/home/thinh/Documents/IS_BigData/DS200.M21-Big-Data/FINAL PROJECT"

python3 -m app.producer_youtube \
  --kafka-servers localhost:9092 \
  --topic youtube_videos \
  --source synthetic \
  --rate 1 \
  --burst-size 5 \
  --num-messages 20

# Expected: Logs showing messages sent to Kafka
# [2026-05-07 ...] INFO Sent 5 messages...
# [2026-05-07 ...] INFO Sent 5 messages...
```

### Terminal 4: Start Streaming Consumer (Make Predictions)

```bash
cd "/home/thinh/Documents/IS_BigData/DS200.M21-Big-Data/FINAL PROJECT"
source .venv_spark/bin/activate

# Set Spark environment
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export SPARK_HOME=/home/thinh/spark
export PYTHONPATH="$PWD:/home/thinh/spark/python:/home/thinh/spark/python/lib/pyspark.zip:/home/thinh/spark/python/lib/py4j-0.10.9.9-src.zip"

# Start streaming processor
python3 -m app.streaming_spark \
  --kafka-servers localhost:9092 \
  --input-topic youtube_videos \
  --output-topic youtube_predictions \
  --model-path "models/rf_model_demo" \
  --checkpoint-dir /tmp/spark_chkpt_youtube

# Expected: Spark initialization + "Reading from youtube_videos topic..."
```

### Terminal 5: Monitor Predictions (Display Consumer)

```bash
cd "/home/thinh/Documents/IS_BigData/DS200.M21-Big-Data/FINAL PROJECT"

python3 -m app.consumer_predictions \
  --kafka-servers localhost:9092 \
  --topic youtube_predictions

# Expected output (streaming predictions):
# [Prediction 1] video_id: xxx, predicted_trending_days: 4.2
# [Prediction 2] video_id: yyy, predicted_trending_days: 2.8
# ...
```

---

## ✅ What We've Verified Working

### ETL Pipeline ✅ (Tested 2026-05-07 14:43-14:45)
```
Input:   data/*videos.csv (600MB+ multi-country)
Process: Feature engineering + Day-1 snapshot
Output:  data/cleaned_youtube_regression.parquet (7.5MB) ✓
Time:    ~2 minutes
```

### Training Pipeline ✅ (Tested 2026-05-07 14:46-14:47)
```
Input:   data/cleaned_youtube_regression.parquet
Process: RandomForestRegressor training
Output:  models/rf_model_demo (256KB) ✓
Metrics: RMSE 2.8459 (vs baseline 3.1751) - 10% better ✓
Time:    ~1 minute
```

### Streaming Code ✅ (Code verified, infrastructure pending)
```
Producer:  app/producer_youtube.py ✓ Ready
Streaming: app/streaming_spark.py ✓ Ready
Consumer:  app/consumer_predictions.py ✓ Ready
```

---

## 🎯 Quick Re-Run Script (After One-Time Setup)

Create `run_full_pipeline_streaming.sh`:

```bash
#!/bin/bash
set -e

cd "/home/thinh/Documents/IS_BigData/DS200.M21-Big-Data/FINAL PROJECT"
source .venv_spark/bin/activate

export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-adf64
export SPARK_HOME=/home/thinh/spark
export PYTHONPATH="$PWD:/home/thinh/spark/python:/home/thinh/spark/python/lib/pyspark.zip:/home/thinh/spark/python/lib/py4j-0.10.9.9-src.zip"

echo "=== STEP 1: ETL Pipeline ==="
python3 -m app.clean_spark_v2_fixed
if [ $? -ne 0 ]; then echo "❌ ETL failed"; exit 1; fi

echo ""
echo "=== STEP 2: Training Pipeline ==="
python3 -m app.app_spark_v2_fixed \
  --data "data/cleaned_youtube_regression.parquet" \
  --num-trees 20 \
  --max-depth 6 \
  --save-model "models/rf_model_demo"
if [ $? -ne 0 ]; then echo "❌ Training failed"; exit 1; fi

echo ""
echo "✅ ML PIPELINE COMPLETE"
echo "   - Cleaned data: data/cleaned_youtube_regression.parquet"
echo "   - Model saved: models/rf_model_demo"
echo ""
echo "📡 NEXT: Run Kafka streaming (see STREAMING_GUIDE below)"
```

---

## 📊 Data Flow Diagram

```
Raw CSV Files (600MB)
    ↓
[ETL: clean_spark_v2_fixed] ← ✅ WORKING
    ↓
Cleaned Parquet (7.5MB)
    ↓
[TRAINING: app_spark_v2_fixed] ← ✅ WORKING
    ↓
Trained Model (256KB)
    ↓
[STREAMING: streaming_spark + Kafka]
    ├─ Producer: youtube_videos topic
    ├─ Spark: Reads + Predicts
    └─ Consumer: youtube_predictions topic
         ↓
    Real-time Predictions ← 🟡 READY (Kafka setup needed)
```

---

## 🐛 Troubleshooting Kafka

### Issue 1: `InconsistentClusterIdException`
**Fix:** See "Known Kafka Issue & Fix" section above (clean storage)

### Issue 2: Connection refused (ECONNREFUSED)
```bash
# Check broker logs
tail -50 /tmp/broker.log

# Verify broker port
netstat -tlnp | grep 9092

# If not listening, restart broker:
pkill -f "kafka.Kafka$"
sleep 5
cd ~/kafka && nohup bin/kafka-server-start.sh config/server.properties > /tmp/broker.log 2>&1 &
```

### Issue 3: Streaming timeout
```bash
# Increase timeout in streaming_spark.py:
# --trigger once <- Use this for testing (single batch)

python3 -m app.streaming_spark \
  --kafka-servers localhost:9092 \
  --input-topic youtube_videos \
  --output-topic youtube_predictions \
  --model-path "models/rf_model_demo" \
  --checkpoint-dir /tmp/spark_chkpt_youtube \
  --trigger once
```

---

## ✅ Verification Checklist

- [x] ETL pipeline creates parquet file
- [x] Training pipeline creates model + shows RMSE improvement
- [x] Model files exist on disk
- [ ] Kafka topics created (requires broker fix)
- [ ] Producer sends messages to Kafka
- [ ] Streaming reads from Kafka
- [ ] Consumer displays predictions

---

## 📝 Complete Timeline (This Session)

```
14:43-14:45  ✅ ETL executed successfully
14:46-14:47  ✅ Training executed successfully
15:01        🟡 Kafka setup started (cluster ID issue discovered)
            → Solution: Clean storage + restart
```

---

## 🎓 Key Commands Reference

| Stage | Command |
|-------|---------|
| **ETL** | `python3 -m app.clean_spark_v2_fixed` |
| **Train** | `python3 -m app.app_spark_v2_fixed --data "data/cleaned_youtube_regression.parquet" --num-trees 20 --max-depth 6 --save-model "models/rf_model_demo"` |
| **Producer** | `python3 -m app.producer_youtube --kafka-servers localhost:9092 --topic youtube_videos --source synthetic --num-messages 20` |
| **Streaming** | `python3 -m app.streaming_spark --kafka-servers localhost:9092 --input-topic youtube_videos --output-topic youtube_predictions --model-path "models/rf_model_demo" --checkpoint-dir /tmp/spark_chkpt_youtube` |
| **Consumer** | `python3 -m app.consumer_predictions --kafka-servers localhost:9092 --topic youtube_predictions` |

---

**Status:** ML pipeline ✅ verified. Streaming infrastructure requires Kafka broker fix.  
**Next Action:** Follow "Known Kafka Issue & Fix" section, then run 6-terminal streaming setup.
