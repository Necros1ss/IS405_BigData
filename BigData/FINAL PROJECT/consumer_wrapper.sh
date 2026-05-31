#!/bin/bash
# Prediction consumer wrapper - reads from youtube_predictions topic

KAFKA_TOPIC="youtube_predictions"

DOCKER_CMD="docker"
if ! docker ps >/dev/null 2>&1; then
    if sudo -n docker ps >/dev/null 2>&1; then
        DOCKER_CMD="sudo docker"
    fi
fi

if ! $DOCKER_CMD ps >/dev/null 2>&1; then
    echo "❌ Cannot access Docker daemon."
    echo "   Run this script with sudo: sudo ./consumer_wrapper.sh"
    echo "   Or add your user to docker group: sudo usermod -aG docker \$USER"
    exit 1
fi

echo "📥 Prediction Consumer - Reading from Kafka"
echo "   Topic: $KAFKA_TOPIC"
echo ""
echo "⏳ Waiting for predictions..."
echo "=================================="
echo ""

# Read predictions from Kafka
$DOCKER_CMD exec kafka kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic $KAFKA_TOPIC \
    --from-beginning \
    --property print.timestamp=true \
    --property print.key=false \
    --max-messages 50 2>/dev/null | while read line; do
    
    if [[ -z "$line" ]]; then
        continue
    fi
    
    echo "📊 $line"
    echo ""
done

echo "=================================="
echo "✅ All predictions displayed"
