#!/bin/bash

# ============================================================
# Quick Reference Guide
# ============================================================

echo "📖 QUICK REFERENCE GUIDE - Kafka + Spark + Streamlit Setup"
echo "==========================================================="
echo ""

echo "🚀 QUICK START COMMANDS:"
echo ""
echo "1️⃣  Start all Docker services:"
echo "    ./start-docker.sh"
echo "    OR: docker-compose up -d"
echo ""

echo "2️⃣  Start YouTube Kafka producer (from host):"
echo "    python app/producer_youtube.py --loop --interval 10"
echo ""

echo "3️⃣  Open Streamlit Dashboard:"
echo "    http://localhost:8501"
echo ""

echo "4️⃣  Monitor Kafka:"
echo "    http://localhost:8080 (Kafka UI)"
echo ""

echo "5️⃣  Monitor Spark:"
echo "    http://localhost:8081 (Spark Master)"
echo ""

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

echo "📊 COMMON COMMANDS:"
echo ""
echo "View logs:"
echo "  docker-compose logs -f                    # All services"
echo "  docker-compose logs -f streamlit          # Streamlit only"
echo "  docker-compose logs -f kafka              # Kafka only"
echo ""

echo "Check status:"
echo "  docker-compose ps                         # Container status"
echo "  docker-compose exec kafka kafka-topics --list --bootstrap-server kafka:29092"
echo ""

echo "Stop/Start:"
echo "  docker-compose stop                       # Stop services"
echo "  docker-compose start                      # Start services"
echo "  docker-compose restart kafka              # Restart Kafka"
echo "  docker-compose down                       # Stop & remove"
echo "  docker-compose down -v                    # Stop & remove data"
echo ""

echo "Execute commands:"
echo "  docker-compose exec streamlit bash        # Shell into container"
echo "  docker-compose exec kafka bash            # Kafka container"
echo ""

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

echo "📈 DATA FLOW:"
echo ""
echo "  Producer (YouTube API Data)"
echo "         ↓"
echo "  Kafka Broker (localhost:9092)"
echo "         ↓"
echo "  Streamlit Consumer"
echo "         ↓"
echo "  Real-time Charts"
echo ""

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

echo "🔌 SERVICE ENDPOINTS:"
echo ""
echo "  Kafka Broker        → localhost:9092"
echo "  Zookeeper          → localhost:2181"
echo "  Kafka UI           → http://localhost:8080"
echo "  Spark Master       → http://localhost:8081"
echo "  Spark Worker       → http://localhost:8082"
echo "  Streamlit          → http://localhost:8501"
echo ""

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

echo "🆘 TROUBLESHOOTING:"
echo ""
echo "❌ Kafka not responding?"
echo "   docker-compose restart kafka"
echo "   docker-compose logs kafka | head -20"
echo ""

echo "❌ Streamlit not updating?"
echo "   - Check producer is running"
echo "   - Verify KAFKA_BROKER env var"
echo "   - docker-compose restart streamlit"
echo ""

echo "❌ Port already in use?"
echo "   lsof -i :8501        # Find process"
echo "   kill -9 <PID>        # Kill process"
echo ""

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

echo "📖 DOCUMENTATION:"
echo ""
echo "  • Docker Setup     → DOCKER_SETUP.md"
echo "  • Streamlit Guide  → STREAMLIT_README.md"
echo "  • Main README      → README.md"
echo ""

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

echo "✅ Ready to start! Run: ./start-docker.sh"
