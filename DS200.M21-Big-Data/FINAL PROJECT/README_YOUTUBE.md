# YouTube Trending Videos Prediction Project

## Project Overview
This is a Big Data project that uses machine learning and Apache Spark to predict whether YouTube videos will become trending based on their characteristics.

**Original Project:** Flight Delays Prediction (converted to YouTube Trending prediction)  
**Dataset:** [Trending YouTube Videos 113 Countries](https://www.kaggle.com/datasets/asaniczka/trending-youtube-videos-113-countries)  
**Technology Stack:** Python, PySpark (batch + Structured Streaming), Kafka, Hadoop HDFS, Apache Spark, KaggleHub

## Project Structure
```
FINAL PROJECT/
├── Predict YouTube Trending.ipynb      # Main Jupyter notebook with PySpark models
├── Preprocessing Data.ipynb             # Data preprocessing notebook
├── app/
│   ├── app_spark.py                    # Batch training / evaluation pipeline
│   ├── streaming_spark.py             # Kafka + Spark Structured Streaming inference
│   ├── producer_youtube.py            # Synthetic Kafka producer
│   └── consumer_predictions.py        # Kafka predictions consumer
├── STREAMING_GUIDE.md                  # Streaming setup and run instructions
├── STREAMING_QUICK_START.md            # Quick reference
└── requirements.txt                    # Python dependencies
```

## Features

### Data Features Used:
- **tag_count**: Number of tags used in the video
- **description_length**: Length of video description
- **like_ratio**: Ratio of likes to views
- **comment_ratio**: Ratio of comments to views
- **engagement**: Total of likes + comments, used for label generation in batch training only

### Target Variable:
- **LABEL (Trending)**: 
  - 1 = Video has above-median engagement (Trending)
  - 0 = Video has below-median engagement (Not Trending)

## Machine Learning Models

The current codebase trains a **Random Forest** Spark ML pipeline for batch modeling, then uses the saved model in a streaming inference job.

Batch training uses:
- RandomForestClassifier
- BinaryClassificationEvaluator with AUC
- Feature importance extraction

Streaming inference uses:
- Kafka source topic for incoming video events
- Spark Structured Streaming for parsing and feature engineering
- PipelineModel loading for realtime predictions
- Kafka sink or console output

## Installation

### Prerequisites
- Python 3.7+
- Apache Spark 3.3.0+ (for PySpark)
- 16GB RAM (recommended for Spark)

### Install Dependencies
```bash
pip install -r requirements.txt
```

## Usage

### Option 1: Run Quick Test/Demo
```bash
python3 run_demo.py
```
This creates sample YouTube data and demonstrates the complete pipeline without needing the full dataset.

### Option 2: Run Jupyter Notebook
```bash
jupyter notebook "Predict YouTube Trending.ipynb"
```
This downloads the actual YouTube dataset using KaggleHub and trains models using PySpark.

### Option 3: Run the Application
```bash
python3 -m app.app_spark --data <path-to-csv> --no-sample --save-model /tmp/rf_model
```
This runs the batch Spark pipeline, trains the model, and can save it for realtime inference.

### Option 4: Run Realtime Streaming
```bash
python3 -m app.streaming_spark --model-path /tmp/rf_model --output console
```
This reads events from Kafka, engineers features, and emits predictions in realtime micro-batches.

## Workflow

### 1. Data Download (KaggleHub)
```python
import kagglehub
path = kagglehub.dataset_download("asaniczka/trending-youtube-videos-113-countries")
```

### 2. Data Loading & Cleaning
- Load CSV files from multiple countries
- Remove duplicates
- Handle missing values
- Feature engineering

### 3. Feature Scaling
Using MinMaxScaler to normalize features for ML models

### 4. Model Training
- Split data into train/test sets
- Train a Random Forest Spark ML model
- Evaluate using AUC and feature importances

### 5. Streaming Predictions
- Producer sends video events to Kafka
- Spark Structured Streaming parses and engineers features
- Loaded model predicts trending in realtime

## Results

The batch pipeline reports AUC and feature importances from the Random Forest model. The streaming job prints realtime predictions with `trending`, `prob_not_trending`, and `prob_trending`.

## Example Prediction
```
Input:
  - tag_count: 30
  - description_length: 800
  - like_ratio: 0.10
  - comment_ratio: 0.06

Output:
  Result: TRENDING
  Confidence: 54%
```

## PySpark Configuration
For realtime streaming, set the Kafka connector package before starting Spark:
```bash
export SPARK_KAFKA_PACKAGES="org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1"
```

## Streaming Run Checklist

Use this checklist when running the realtime pipeline end-to-end on Linux:

1. Start ZooKeeper.
```bash
cd ~/kafka
bin/zookeeper-server-start.sh config/zookeeper.properties
```

2. Start the Kafka broker.
```bash
cd ~/kafka
bin/kafka-server-start.sh config/server.properties
```

3. Create the Kafka topics.
```bash
cd ~/kafka
bin/kafka-topics.sh --create --bootstrap-server localhost:9092 \
  --topic youtube_videos --partitions 1 --replication-factor 1
bin/kafka-topics.sh --create --bootstrap-server localhost:9092 \
  --topic youtube_predictions --partitions 1 --replication-factor 1
```

4. Make sure the batch model exists.
```bash
cd "/home/thinh/Documents/IS_BigData/DS200.M21-Big-Data/FINAL PROJECT"
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export SPARK_HOME=/home/thinh/spark
export PYTHONPATH="$PWD:$SPARK_HOME/python:$SPARK_HOME/python/lib/pyspark.zip:$SPARK_HOME/python/lib/py4j-0.10.9.9-src.zip"
export SPARK_KAFKA_PACKAGES="org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1"

python3 -m app.app_spark --data app/data_final/youtube_large_sample.csv \
  --no-sample --save-model /tmp/rf_model
```

5. Start the streaming job.
```bash
cd "/home/thinh/Documents/IS_BigData/DS200.M21-Big-Data/FINAL PROJECT"
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export SPARK_HOME=/home/thinh/spark
export SPARK_LOCAL_HOSTNAME=localhost
export PYTHONPATH="$PWD:$SPARK_HOME/python:$SPARK_HOME/python/lib/pyspark.zip:$SPARK_HOME/python/lib/py4j-0.10.9.9-src.zip"
export SPARK_KAFKA_PACKAGES="org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1"

python3 -m app.streaming_spark --kafka-servers localhost:9092 \
  --input-topic youtube_videos \
  --output-topic youtube_predictions \
  --model-path /tmp/rf_model \
  --output kafka
```

6. Start the consumer.
```bash
cd "/home/thinh/Documents/IS_BigData/DS200.M21-Big-Data/FINAL PROJECT"
python3 -m app.consumer_predictions --kafka-servers localhost:9092 --topic youtube_predictions
```

7. Start the producer after the streaming job is already listening.
```bash
cd "/home/thinh/Documents/IS_BigData/DS200.M21-Big-Data/FINAL PROJECT"
python3 -m app.producer_youtube --kafka-servers localhost:9092 --topic youtube_videos --rate 1 --num-messages 5
```

8. Confirm the consumer prints `TRENDING` or `NOT TRENDING` lines.

9. Stop the terminals in reverse order when you finish.

## Data Processing Pipeline

1. **Spark Read CSV** → Load YouTube data from multiple countries
2. **DataFrame Operations** → Select relevant columns, remove nulls
3. **Feature Engineering** → Create ratios and engagement metrics
4. **Vector Assembly** → Combine features into ML vectors
5. **Model Training** → RandomForest on Spark ML
6. **Evaluation** → AUC and feature importances
7. **Kafka Producer** → Send events to `youtube_videos`
8. **Spark Structured Streaming** → Parse, engineer, and predict in realtime
9. **Kafka/Console Sink** → Emit prediction events

## Key Differences from Original Project

| Aspect | Original (Flight Delays) | Current (YouTube Trending) |
|--------|--------------------------|---------------------------|
| Target | Predict departure delay | Predict if video will trend |
| Features | Flight info (time, distance, etc) | Video metrics (likes, comments, tags) |
| Dataset | Flight records | YouTube videos metadata |
| Data Source | CSV files | CSV batch + Kafka stream |
| Models | Classification (3 delay classes) | Binary classification with Spark ML |

## Files Description

### Main Notebooks
- **Predict YouTube Trending.ipynb** 
  - Full end-to-end ML pipeline using PySpark
  - Downloads data from KaggleHub
  - Trains and compares 4 models
  - ~2000 lines of code

- **Preprocessing Data.ipynb**
  - Detailed data cleaning procedures
  - Exploratory Data Analysis (EDA)
  - Data quality checks

### Application Files
- **app/app_spark.py** - Batch Spark ML training and export pipeline
- **app/streaming_spark.py** - Realtime Kafka → Spark → prediction pipeline
- **app/producer_youtube.py** - Synthetic Kafka event producer for demo
- **app/consumer_predictions.py** - Realtime Kafka consumer for output
- **Predict YouTube Trending.ipynb** - Complete Jupyter analysis

## Performance Metrics

The optimized Spark ML pipeline (RandomForest) achieves:
- **AUC**: reported by batch evaluation
- **Training Time**: depends on dataset size and cluster resources
- **Prediction Time**: micro-batch streaming latency
- **Scalability**: Horizontal via Spark + Kafka + HDFS

## Troubleshooting

### Issue: KaggleHub not found
**Solution:** Install with `pip install kagglehub`

### Issue: Spark memory error
**Solution:** Reduce executor memory or dataset size in config

### Issue: Missing dependencies
**Solution:** Run `pip install -r requirements.txt`

### Issue: Kafka connector not found
**Solution:** Set `SPARK_KAFKA_PACKAGES=org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1` before starting streaming

## Future Improvements

1. Add more features (upload time, channel age, previous videos)
2. Use deep learning models (LSTM, Transformer)
3. Implement real-time prediction API
4. Add geographic trending analysis
5. Implement A/B testing for model improvements
6. Deploy to cloud (AWS, GCP)
7. Replace synthetic Kafka producer with a real ingest source

## References

- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Scikit-learn Guide](https://scikit-learn.org/stable/user_guide.html)
- [Kaggle YouTube Dataset](https://www.kaggle.com/datasets/asaniczka/trending-youtube-videos-113-countries)

## Author
Phạm Đức Thể (19522253)  
Class: DS200.M21

## License
This project is for educational purposes.

---
**Last Updated:** May 2026
**Status:** ✓ Fully Converted from Flight Delays to YouTube Trending Prediction
