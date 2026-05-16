#!/bin/bash
# Quick status check script

DOCKER_CMD="docker"
if ! docker ps >/dev/null 2>&1; then
    if sudo -n docker ps >/dev/null 2>&1; then
        DOCKER_CMD="sudo docker"
    fi
fi

if ! $DOCKER_CMD ps >/dev/null 2>&1; then
    echo "❌ Cannot access Docker daemon."
    echo "   Run this script with sudo: sudo ./status.sh"
    echo "   Or add your user to docker group: sudo usermod -aG docker \$USER"
    exit 1
fi

echo "╔════════════════════════════════════════════════════════════╗"
echo "║     🎬 YOUTUBE STREAMING PIPELINE - STATUS CHECK          ║"
echo "╚════════════════════════════════════════════════════════════╝"
echo ""

# Services
echo "📦 SERVICES STATUS"
echo "─────────────────────────────────────────────────────────────"
$DOCKER_CMD ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" | grep -E "kafka|zookeeper|kafka-ui|streamlit" | awk '{printf "  %-15s %s\n", $1, $2}'
echo ""

# Topics
echo "📋 KAFKA TOPICS"
echo "─────────────────────────────────────────────────────────────"
echo "  Available topics:"
$DOCKER_CMD exec kafka kafka-topics --list --bootstrap-server localhost:9092 2>/dev/null | grep -v "^__" | awk '{printf "    ✓ %s\n", $0}'
echo ""

# Message count
echo "📊 DATA FLOW"
echo "─────────────────────────────────────────────────────────────"
VIDEO_COUNT=$($DOCKER_CMD exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --group console-consumer-* --describe 2>/dev/null | tail -1 | awk '{print $4}' || echo "?")
echo "  Videos in kafka topic: [Run Kafka UI dashboard for live count]"
echo "  Dashboard URL: http://localhost:8080"
echo ""

# Producer status
echo "🎥 PRODUCER STATUS"
echo "─────────────────────────────────────────────────────────────"
if pgrep -f "producer_wrapper.sh" > /dev/null; then
    echo "  ✅ Producer running (PID: $(pgrep -f 'producer_wrapper.sh'))"
    echo "  📤 Sending data every 10 seconds to: youtube_videos topic"
else
    echo "  ❌ Producer NOT running"
    echo "  💡 Start with: cd \"FINAL PROJECT\" && ./producer_wrapper.sh 10"
fi
echo ""

# Commands reference
echo "📝 QUICK COMMANDS"
echo "─────────────────────────────────────────────────────────────"
echo "  View Kafka messages:"
echo "    docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 \\"
echo "      --topic youtube_videos --max-messages 5"
echo ""
echo "  View all services:"
echo "    docker-compose ps"
echo ""
echo "  View logs:"
echo "    docker logs kafka -f          # Kafka logs"
echo "    docker logs kafka-ui -f       # Kafka UI logs"
echo ""

# Endpoints
echo "🌐 ENDPOINTS"
echo "─────────────────────────────────────────────────────────────"
echo "  Kafka Broker ........... localhost:9092"
echo "  Zookeeper .............. localhost:2181"
echo "  Kafka UI Dashboard ..... http://localhost:8080  ⭐ Open in browser"
echo "  Streamlit (when running) http://localhost:8501"
echo ""

echo "╚════════════════════════════════════════════════════════════╝"
