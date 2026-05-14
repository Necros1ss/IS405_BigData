#!/bin/bash

# Docker startup script for Kafka + Spark + Streamlit ecosystem

set -e

echo "🚀 Starting Big Data Streaming Platform..."
echo "==========================================="

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo "❌ Docker is not installed. Please install Docker first."
    exit 1
fi

# Check if docker-compose is installed
if ! command -v docker-compose &> /dev/null; then
    echo "❌ docker-compose is not installed. Please install it first."
    exit 1
fi

# Build images
echo "🔨 Building Docker images..."
docker-compose build --no-cache

# Start services
echo "🌍 Starting services..."
docker-compose up -d

# Wait for services to be ready
echo "⏳ Waiting for services to be ready..."
sleep 10

# Check Kafka connectivity
echo "🔍 Checking Kafka connectivity..."
docker-compose exec -T kafka kafka-broker-api-versions --bootstrap-server localhost:9092 || {
    echo "⚠️  Kafka not ready yet, waiting..."
    sleep 10
}

# Create Kafka topic if it doesn't exist
echo "📝 Ensuring Kafka topic exists..."
docker-compose exec -T kafka kafka-topics --create \
    --bootstrap-server localhost:9092 \
    --topic youtube_videos \
    --partitions 3 \
    --replication-factor 1 \
    --if-not-exists 2>/dev/null || echo "Topic already exists or Kafka not ready"

echo ""
echo "✅ Platform started successfully!"
echo ""
echo "📊 Available Services:"
echo "  • Zookeeper       → localhost:2181"
echo "  • Kafka Broker    → localhost:9092"
echo "  • Kafka UI        → http://localhost:8080"
echo "  • Spark Master    → http://localhost:8081"
echo "  • Streamlit       → http://localhost:8501"
echo ""
echo "📖 Next steps:"
echo "  1. Start the producer: python app/producer_youtube.py"
echo "  2. View dashboard: http://localhost:8501"
echo "  3. Monitor Kafka: http://localhost:8080"
echo "  4. Monitor Spark: http://localhost:8081"
echo ""
echo "🛑 To stop all services: docker-compose down"
echo "🧹 To remove all data: docker-compose down -v"
