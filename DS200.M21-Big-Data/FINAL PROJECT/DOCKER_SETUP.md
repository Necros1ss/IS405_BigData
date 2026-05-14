# 🐳 Docker Setup Guide - Kafka + Spark + Streamlit

## 📋 Overview

This Docker setup provides a complete containerized environment for:
- **Kafka** - Message streaming platform with Zookeeper coordination
- **Spark** - Distributed data processing (Master + 1 Worker)
- **Kafka UI** - Web interface for Kafka monitoring
- **Streamlit** - Real-time dashboard for data visualization

## 🚀 Quick Start

### 1. Prerequisites
```bash
# Install Docker & Docker Compose
# Ubuntu/Debian:
sudo apt-get install docker.io docker-compose

# macOS:
brew install docker docker-compose

# Verify installation
docker --version
docker-compose --version
```

### 2. Start All Services
```bash
# Method 1: Using startup script (Recommended)
chmod +x start-docker.sh
./start-docker.sh

# Method 2: Manual start
docker-compose up -d
```

### 3. Verify Services
```bash
# Check all containers
docker-compose ps

# View logs
docker-compose logs -f

# View specific service logs
docker-compose logs -f kafka
docker-compose logs -f streamlit
```

## 🔌 Service Endpoints

| Service | URL/Port | Purpose |
|---------|----------|---------|
| **Kafka Broker** | `localhost:9092` | Producer/Consumer connection |
| **Zookeeper** | `localhost:2181` | Kafka coordination |
| **Kafka UI** | `http://localhost:8080` | Monitor topics & messages |
| **Spark Master** | `http://localhost:8081` | Spark cluster dashboard |
| **Spark Worker** | `http://localhost:8082` | Worker node dashboard |
| **Streamlit Dashboard** | `http://localhost:8501` | Real-time visualization |

## 📊 Workflow

### Step 1: Start Producer (YouTube Data)
```bash
# From host machine (outside Docker)
python app/producer_youtube.py --loop --interval 10

# Or run inside container
docker-compose exec -it spark-master python /app/producer_youtube.py
```

### Step 2: Monitor Kafka
- Open browser: `http://localhost:8080`
- View `youtube_videos` topic
- See real-time messages coming in

### Step 3: View Streamlit Dashboard
- Open browser: `http://localhost:8501`
- Dashboard automatically reads from Kafka
- Real-time charts update as data arrives

### Step 4: Monitor Spark
- Spark Master UI: `http://localhost:8081`
- Worker status: `http://localhost:8082`

## 🔧 Common Commands

### Start Services
```bash
docker-compose up -d
```

### Stop Services (keep data)
```bash
docker-compose stop
```

### Restart Services
```bash
docker-compose restart
```

### View Logs
```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f streamlit
docker-compose logs -f kafka

# Follow new logs only
docker-compose logs --tail=50 -f kafka
```

### Execute Commands in Container
```bash
# Streamlit container
docker-compose exec streamlit bash

# Kafka container - create topic
docker-compose exec kafka kafka-topics --list --bootstrap-server kafka:9092

# List Kafka topics
docker-compose exec kafka kafka-topics --list --bootstrap-server localhost:9092

# Delete Kafka topic
docker-compose exec kafka kafka-topics --delete --bootstrap-server localhost:9092 --topic youtube_videos

# Spark container
docker-compose exec spark-master spark-shell
```

### Clean Up

```bash
# Stop and remove containers (keep volumes)
docker-compose down

# Stop, remove containers AND volumes (delete all data)
docker-compose down -v

# Remove all unused Docker resources
docker system prune -a
```

## 📊 Data Flow Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Host Machine                             │
│  python producer_youtube.py ──→ Produces YouTube data       │
└────────────────────────┬────────────────────────────────────┘
                         │
              ┌──────────▼───────────┐
              │   Docker Network     │
              │  (bigdata-network)   │
              └──────────┬───────────┘
                         │
         ┌───────────────┼───────────────┐
         │               │               │
    ┌────▼────┐    ┌─────▼──────┐   ┌──▼──────┐
    │ Zookeeper│    │   Kafka    │   │  Kafka  │
    │          │    │   Broker   │   │   UI    │
    │ :2181    │    │  :9092     │   │  :8080  │
    └──────────┘    └─────┬──────┘   └─────────┘
                         │
         ┌───────────────┼───────────────┐
         │               │               │
    ┌────▼──────┐  ┌─────▼──────┐  ┌───▼────────┐
    │   Spark   │  │   Spark    │  │ Streamlit  │
    │  Master   │  │  Worker-1  │  │ Dashboard  │
    │  :8081    │  │  :8082     │  │  :8501     │
    └───────────┘  └────────────┘  └────────────┘
```

## 📈 Streamlit Dashboard Features

The Streamlit dashboard includes:

- **Real-time Views Timeline** - Line chart of video views over time
- **Engagement Metrics** - Normalized comparison of views, likes, and comments
- **Distribution Analysis** - Histograms showing metric distributions
- **Statistics Summary** - Mean, max, min, and std dev for all metrics
- **Latest Videos** - List of recently received videos

### Dashboard Refresh Rates
- Auto-refresh every 1 second (configurable)
- Stores last 100 videos and 500 data points
- Real-time Kafka consumer in background thread

## 🔍 Troubleshooting

### Kafka not connecting
```bash
# Check if Kafka is running
docker-compose ps

# Check Kafka logs
docker-compose logs kafka

# Test connectivity
docker-compose exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092
```

### Streamlit not updating
```bash
# Check Streamlit logs
docker-compose logs streamlit

# Ensure Kafka broker address is correct
docker-compose exec streamlit echo $KAFKA_BROKER
```

### Port already in use
```bash
# Find process using port 9092
lsof -i :9092

# Kill the process or change port in docker-compose.yml
```

### Memory issues
Edit `docker-compose.yml` and adjust worker memory:
```yaml
SPARK_WORKER_MEMORY: 1G  # Reduce from 2G
```

## 📚 Environment Variables

Override defaults by creating `.env` file:
```bash
KAFKA_BROKER=kafka:29092
YOUTUBE_TOPIC=youtube_videos
SPARK_MASTER_URL=spark://spark-master:7077
SPARK_WORKER_MEMORY=2G
SPARK_WORKER_CORES=2
```

## 🔗 Important Links

- **Docker Compose Docs**: https://docs.docker.com/compose/
- **Kafka Docs**: https://kafka.apache.org/documentation/
- **Apache Spark Docs**: https://spark.apache.org/docs/latest/
- **Streamlit Docs**: https://docs.streamlit.io/
- **Confluent Docker Images**: https://hub.docker.com/u/confluentinc

## 📝 Notes

- Kafka retains 24 hours of data (configurable in docker-compose.yml)
- Spark workers have 2GB memory each (tune for your system)
- Zookeeper uses port 2181 for internal coordination
- Network is isolated using `bigdata-network` bridge network

## 🎯 Next Steps

1. ✅ Start Docker services
2. ✅ Start Kafka producer
3. ✅ Monitor in Kafka UI
4. ✅ View Streamlit dashboard
5. ✅ Process data with Spark jobs
6. ✅ Analyze metrics and trends
