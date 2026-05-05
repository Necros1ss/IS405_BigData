#!/bin/bash
# System Environment Check - Kiểm tra môi trường hệ thống
# Kiểm tra Spark, Hadoop/HDFS, Kafka, Java, Python, etc.

echo "=========================================="
echo "🔍 KIỂM TRA MÔI TRƯỜNG HỆ THỐNG"
echo "=========================================="
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

check_command() {
    if command -v "$1" &> /dev/null; then
        echo -e "${GREEN}✓${NC} $1 found"
        $1 --version 2>/dev/null || $1 -version 2>/dev/null || echo ""
        return 0
    else
        echo -e "${RED}✗${NC} $1 NOT found"
        return 1
    fi
}

check_dir() {
    if [ -d "$1" ]; then
        echo -e "${GREEN}✓${NC} $1 exists"
        du -sh "$1" 2>/dev/null | awk '{print "  Size: " $1}'
        return 0
    else
        echo -e "${RED}✗${NC} $1 NOT found"
        return 1
    fi
}

echo "═══════════════════════════════════════════"
echo "1️⃣  JAVA & RUNTIME"
echo "═══════════════════════════════════════════"
echo ""
check_command java
echo ""

echo "═══════════════════════════════════════════"
echo "2️⃣  PYTHON & PIP"
echo "═══════════════════════════════════════════"
echo ""
check_command python3
echo ""
check_command pip3
echo ""

echo "═══════════════════════════════════════════"
echo "3️⃣  APACHE SPARK"
echo "═══════════════════════════════════════════"
echo ""
check_command spark-submit
echo ""
check_dir "/home/thinh/spark"
echo ""
check_dir "/opt/spark"
echo ""
check_dir "/usr/local/spark"
echo ""

echo "═══════════════════════════════════════════"
echo "4️⃣  HADOOP / HDFS"
echo "═══════════════════════════════════════════"
echo ""
check_command hdfs
echo ""
check_command hadoop
echo ""
check_dir "/opt/hadoop"
echo ""
check_dir "/opt/hadoop-3.3.6"
echo ""
check_dir "/usr/local/hadoop"
echo ""

# Check if HDFS is running
echo ""
echo "Checking HDFS daemons:"
if command -v jps &> /dev/null; then
    echo "Java processes:"
    jps 2>/dev/null || echo "  (jps not accessible)"
    echo ""
else
    echo "  (jps not available)"
fi

echo "═══════════════════════════════════════════"
echo "5️⃣  APACHE KAFKA"
echo "═══════════════════════════════════════════"
echo ""
check_command kafka-broker-api-versions.sh
echo ""
check_command kafka-console-producer.sh
echo ""
check_dir "/opt/kafka"
echo ""
check_dir "/usr/local/kafka"
echo ""

echo "═══════════════════════════════════════════"
echo "6️⃣  GIT"
echo "═══════════════════════════════════════════"
echo ""
check_command git
echo ""

echo "═══════════════════════════════════════════"
echo "7️⃣  ENVIRONMENT VARIABLES"
echo "═══════════════════════════════════════════"
echo ""

if [ -n "$JAVA_HOME" ]; then
    echo -e "${GREEN}✓${NC} JAVA_HOME = $JAVA_HOME"
else
    echo -e "${YELLOW}⚠${NC} JAVA_HOME not set"
fi

if [ -n "$SPARK_HOME" ]; then
    echo -e "${GREEN}✓${NC} SPARK_HOME = $SPARK_HOME"
else
    echo -e "${YELLOW}⚠${NC} SPARK_HOME not set"
fi

if [ -n "$HADOOP_HOME" ]; then
    echo -e "${GREEN}✓${NC} HADOOP_HOME = $HADOOP_HOME"
else
    echo -e "${YELLOW}⚠${NC} HADOOP_HOME not set"
fi

if [ -n "$KAFKA_HOME" ]; then
    echo -e "${GREEN}✓${NC} KAFKA_HOME = $KAFKA_HOME"
else
    echo -e "${YELLOW}⚠${NC} KAFKA_HOME not set"
fi

echo ""

echo "═══════════════════════════════════════════"
echo "8️⃣  PYTHON PACKAGES (PySpark related)"
echo "═══════════════════════════════════════════"
echo ""

check_python_package() {
    if python3 -c "import $1" 2>/dev/null; then
        version=$(python3 -c "import $1; print(getattr($1, '__version__', 'unknown'))" 2>/dev/null)
        echo -e "${GREEN}✓${NC} $1 (version: $version)"
    else
        echo -e "${RED}✗${NC} $1 NOT installed"
    fi
}

check_python_package "pyspark"
check_python_package "pandas"
check_python_package "numpy"
check_python_package "matplotlib"
check_python_package "kafka"

echo ""

echo "═══════════════════════════════════════════"
echo "📊 SYSTEM INFO"
echo "═══════════════════════════════════════════"
echo ""
echo "Hostname: $(hostname)"
echo "OS: $(uname -s)"
echo "Kernel: $(uname -r)"
echo "CPU cores: $(nproc)"
echo "Memory: $(free -h | grep Mem | awk '{print $2}')"
echo "Disk: $(df -h / | awk 'NR==2 {print $2}')"

echo ""
echo "════════════════════════════════════════════"
echo "✓ Check completed!"
echo "════════════════════════════════════════════"
