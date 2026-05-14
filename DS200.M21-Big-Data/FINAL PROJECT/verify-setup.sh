#!/bin/bash

# ============================================================
# Installation and Setup Verification Checklist
# ============================================================

echo "🔍 Verification Checklist for Docker + Kafka + Streamlit Setup"
echo "=============================================================="
echo ""

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

check_file() {
    local file=$1
    local name=$2
    if [ -f "$file" ]; then
        echo -e "${GREEN}✓${NC} $name"
        return 0
    else
        echo -e "${RED}✗${NC} $name (Missing: $file)"
        return 1
    fi
}

check_command() {
    local cmd=$1
    local name=$2
    if command -v "$cmd" &> /dev/null; then
        echo -e "${GREEN}✓${NC} $name"
        return 0
    else
        echo -e "${RED}✗${NC} $name (Not installed)"
        return 1
    fi
}

# ============================================================
echo "📦 Checking Prerequisites..."
echo "---"
check_command "docker" "Docker installed"
check_command "docker-compose" "Docker Compose installed"
echo ""

# ============================================================
echo "📄 Checking Configuration Files..."
echo "---"
check_file "docker-compose.yml" "docker-compose.yml"
check_file "Dockerfile" "Dockerfile"
check_file "Dockerfile.streamlit" "Dockerfile.streamlit"
check_file ".dockerignore" ".dockerignore"
check_file "app/streamlit_dashboard.py" "Streamlit dashboard app"
check_file "requirements-streamlit.txt" "Streamlit requirements"
check_file "requirements.txt" "Main requirements"
echo ""

# ============================================================
echo "📖 Checking Documentation..."
echo "---"
check_file "DOCKER_SETUP.md" "Docker setup guide"
check_file "STREAMLIT_README.md" "Streamlit documentation"
check_file "IMPLEMENTATION_SUMMARY.md" "Implementation summary"
check_file "QUICK_START.sh" "Quick start reference"
echo ""

# ============================================================
echo "🚀 Checking Startup Scripts..."
echo "---"
if [ -x "start-docker.sh" ]; then
    echo -e "${GREEN}✓${NC} start-docker.sh (executable)"
else
    echo -e "${YELLOW}⚠${NC} start-docker.sh (not executable - fixing...)"
    chmod +x start-docker.sh
    echo -e "${GREEN}✓${NC} start-docker.sh (made executable)"
fi
echo ""

# ============================================================
echo "🔧 Checking Application Code..."
echo "---"
check_file "app/config.py" "Kafka configuration"
check_file "app/producer_youtube.py" "YouTube producer"
check_file "app/spark_bootstrap.py" "Spark bootstrap"
echo ""

# ============================================================
echo "✅ Verification Complete!"
echo "=============================================================="
echo ""

echo "📋 Next Steps:"
echo "1. Start Docker services:     ./start-docker.sh"
echo "2. Start Kafka producer:      python app/producer_youtube.py --loop"
echo "3. Open Streamlit:            http://localhost:8501"
echo "4. Open Kafka UI:             http://localhost:8080"
echo "5. View Spark Master:         http://localhost:8081"
echo ""

echo "📖 Quick Reference:            ./QUICK_START.sh"
echo ""

echo "🎯 For detailed setup:         cat DOCKER_SETUP.md"
echo "🎨 For dashboard features:     cat STREAMLIT_README.md"
echo ""
