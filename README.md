# 📈 Real-time YouTube Trending Prediction Pipeline

![Python](https://img.shields.io/badge/Python-3.8%2B-blue)
![Apache Spark](https://img.shields.io/badge/Apache_Spark-3.x-orange)
![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-Streaming-black)

An end-to-end Big Data pipeline for predicting whether a YouTube video will become trending using Apache Spark, Kafka, and Machine Learning.

This project combines batch processing and realtime streaming to build a scalable analytics and prediction system for YouTube trending videos.

- **Course:** IS405.Q23 / DS200.M21 - Big Data
- **Lecturer:** TS Nguyễn Hồ Duy Trí
- **Student:** Trần Hữu Thịnh

---

# 🎯 Project Objectives

- **Big Data Pipeline:** Build a scalable ETL pipeline for processing historical and realtime YouTube video data.
- **Machine Learning:** Train Spark ML models (Random Forest/XGBoost) to predict trending probability and trending duration.
- **Realtime Streaming:** Perform live prediction using Kafka and Spark Structured Streaming.
- **Data Ingestion:** Continuously ingest YouTube metadata using YouTube Data API v3.
- **Analytics:** Process and analyze trending behaviors from streaming events.

---

# 🏗 System Architecture

```text
YouTube API
    ↓
Kafka Producer
    ↓
Kafka Topic (youtube_videos)
    ↓
Spark Structured Streaming
    ↓
ML Prediction Pipeline
    ↓
Kafka Topic (youtube_predictions)
    ↓
Consumer / Dashboard
```

---

# 🛠 Tech Stack

## Big Data & Streaming
- Apache Spark
- Spark Structured Streaming
- Apache Kafka
- Spark MLlib

## Programming & Libraries
- Python
- Pandas
- NumPy
- PyArrow

## Data Formats
- Parquet
- JSON
- CSV

---

# ✨ Features

- Realtime YouTube data ingestion
- Kafka-based streaming architecture
- Spark Structured Streaming processing
- Machine learning prediction pipeline
- Trending video classification
- Realtime prediction output
- Historical batch training
- Configurable Kafka producer
- YouTube Trending API integration

---

# 📂 Repository Structure

```text
FINAL PROJECT/
├── app/             # Core application code (model training, streaming, producer, consumer)
├── data/            # Local data storage (Raw & Cleaned datasets)
├── models/          # Saved Spark MLlib models (Random Forest, etc.)
├── scripts/         # Shell scripts for Kafka environment setup and reset
├── Images/          # Metrics, diagrams, screenshots
├── .env             # Environment variables (contains YOUTUBE_API_KEY)
└── README.md        # Detailed step-by-step execution guide
```

---

# 🚀 Pipeline Workflow

## 1. Batch Training

- Load historical YouTube trending datasets
- Perform preprocessing and feature engineering
- Train Random Forest/XGBoost models using Spark ML
- Save trained models for streaming inference

## 2. Realtime Streaming

- Fetch trending videos from YouTube API
- Publish events into Kafka
- Consume events using Spark Structured Streaming
- Apply trained ML model for realtime prediction
- Publish prediction results to Kafka consumers

---

# 📊 Example Streaming Output

```text
#0001 Published: nCElUD0jvgo | 2,917,604 views
#0002 Published: _SSHB7XPzdI | 760,052 views
#0003 Published: OvhR_tkz9mk | 1,491,286 views
```

---

# ⚡ Quick Start

Detailed setup and execution instructions are available in:

```text
FINAL PROJECT/README.md
```

The detailed guide includes:
1. Spark model training
2. Kafka setup
3. Topic creation
4. Streaming prediction pipeline
5. Kafka consumers
6. YouTube producer execution

---

# 🔑 Environment Variables

Create a `.env` file inside `FINAL PROJECT/`:

```env
YOUTUBE_API_KEY=your_api_key_here
```

---

# 🔮 Future Improvements

- Streamlit/Grafana realtime dashboard
- Docker deployment
- Airflow orchestration
- Distributed Kafka cluster
- Advanced feature engineering
- Model monitoring and observability
- Cloud deployment

---

# 📚 Course Information

IS405.Q23 / DS200.M21 - Big Data  
University of Information Technology - VNUHCM