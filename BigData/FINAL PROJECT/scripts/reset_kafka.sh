#!/bin/bash
# Reset Kafka + Zookeeper Docker containers
# Clears state to fix InconsistentClusterIdException

export DOCKER_HOST=tcp://10.0.2.2:2375

echo "🔄 Resetting Kafka + Zookeeper..."
echo ""

# Stop containers
echo "1️⃣  Stopping containers..."
docker-compose stop zookeeper kafka 2>/dev/null || true

# Remove volumes
echo "2️⃣  Removing volumes..."
docker volume rm finalproject_kafka-data 2>/dev/null || true

# Start fresh
echo "3️⃣  Starting fresh containers..."
docker-compose up -d zookeeper kafka

# Wait for startup
echo "4️⃣  Waiting for services..."
sleep 10

# Create topics
echo "5️⃣  Creating topics..."
docker exec kafka kafka-topics --create --topic youtube_videos \
  --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 --if-not-exists 2>/dev/null

docker exec kafka kafka-topics --create --topic youtube_predictions \
  --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 --if-not-exists 2>/dev/null

echo ""
echo "✅ Kafka + Zookeeper reset complete!"
