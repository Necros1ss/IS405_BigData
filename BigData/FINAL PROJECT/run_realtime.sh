#!/bin/bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="$PROJECT_ROOT/logs"
mkdir -p "$LOG_DIR"

STAMP="$(date +%Y%m%d_%H%M%S)"
LAUNCH_LOG="$LOG_DIR/run_realtime_${STAMP}.log"
STREAMLIT_LOG="$LOG_DIR/streamlit_container_${STAMP}.log"

exec > >(tee -a "$LAUNCH_LOG") 2>&1

MODEL_PATH="${MODEL_PATH:-$PROJECT_ROOT/models/rf_regression_model}"
KAFKA_BROKER="${KAFKA_BROKER:-localhost:9092}"
INPUT_TOPIC="${INPUT_TOPIC:-youtube_videos}"
OUTPUT_TOPIC="${OUTPUT_TOPIC:-youtube_predictions}"
REGION_CODE="${REGION_CODE:-VN}"
MAX_RESULTS="${MAX_RESULTS:-100}"
POLL_INTERVAL="${POLL_INTERVAL:-20}"
CHECKPOINT_DIR="${CHECKPOINT_DIR:-/tmp/spark_chkpt_youtube}"

DOCKER_CMD="docker"
if ! docker ps >/dev/null 2>&1; then
    if sudo -n docker ps >/dev/null 2>&1; then
        DOCKER_CMD="sudo docker"
    fi
fi

if ! $DOCKER_CMD ps >/dev/null 2>&1; then
    echo "❌ Cannot access Docker daemon."
    echo "   Run with sudo: sudo ./run_realtime.sh"
    echo "   Or add your user to docker group: sudo usermod -aG docker \$USER"
    exit 1
fi

if [[ ! -d "$MODEL_PATH" ]]; then
    echo "❌ Model not found at: $MODEL_PATH"
    echo "   Train first or copy the model to that path."
    exit 1
fi

echo "🚀 Starting realtime pipeline"
echo "   Project root : $PROJECT_ROOT"
echo "   Model path   : $MODEL_PATH"
echo "   Topics       : $INPUT_TOPIC -> $OUTPUT_TOPIC"
echo "   Logs         : $LOG_DIR"
echo "   Launcher log : $LAUNCH_LOG"
echo "   Streamlit log: $STREAMLIT_LOG"
echo ""

# --- Ensure Spark Python libs are available when using system Spark ---
SPARK_HOME="${SPARK_HOME:-/home/thinh/spark}"
if [[ -d "$SPARK_HOME/python" ]]; then
  PY4J_ZIP=""
  for f in "$SPARK_HOME"/python/lib/py4j*.zip "$SPARK_HOME"/python/lib/py4j*.egg "$SPARK_HOME"/python/lib/py4j*src.zip; do
    [[ -e "$f" ]] && { PY4J_ZIP="$f"; break; }
  done
  if [[ -n "$PY4J_ZIP" ]]; then
    export PYTHONPATH="$SPARK_HOME/python:$PY4J_ZIP:$PROJECT_ROOT${PYTHONPATH:+:$PYTHONPATH}"
    echo "   ↳ Added Spark Python libs to PYTHONPATH (SPARK_HOME=$SPARK_HOME)"
  else
    export PYTHONPATH="$SPARK_HOME/python:$PROJECT_ROOT${PYTHONPATH:+:$PYTHONPATH}"
    echo "   ↳ Added $SPARK_HOME/python to PYTHONPATH (py4j not found in lib)"
  fi
else
  export PYTHONPATH="$PROJECT_ROOT${PYTHONPATH:+:$PYTHONPATH}"
fi

# Ensure the project virtualenv has small runtime deps (requests) if present
VENV_PY="$PROJECT_ROOT/.venv/bin/python"
if [[ -x "$VENV_PY" ]]; then
  if ! "$VENV_PY" - <<'PY_CHECK'
import importlib, sys
try:
    importlib.import_module('requests')
except Exception:
    sys.exit(1)
else:
    sys.exit(0)
PY_CHECK
  then
    echo "   ↳ Installing missing runtime package 'requests' into .venv"
    "$VENV_PY" -m pip install --no-input --upgrade pip setuptools wheel >/dev/null 2>&1 || true
    "$VENV_PY" -m pip install --no-input --break-system-packages requests || true
  fi
fi

# --- Ensure JAVA_HOME points to a Java 17 runtime if available ---
if [[ -z "${JAVA_HOME:-}" ]]; then
  for candidate in /usr/lib/jvm/java-17-openjdk-amd64 /usr/lib/jvm/java-17-openjdk /usr/lib/jvm/java-17*; do
    if [[ -x "$candidate/bin/java" ]]; then
      JAVA_HOME="$candidate"
      break
    fi
  done
fi
if [[ -n "${JAVA_HOME:-}" && -x "$JAVA_HOME/bin/java" ]]; then
  export JAVA_HOME
  export PATH="$JAVA_HOME/bin:$PATH"
  JVER=$($JAVA_HOME/bin/java -version 2>&1 | awk -F\" '/version/ {print $2}' | cut -d. -f1)
  echo "   ↳ Using JAVA_HOME=$JAVA_HOME (java major=$JVER)"
  if [[ "$JVER" -lt 17 ]]; then
    echo "   ⚠️ JAVA version < 17 detected at JAVA_HOME ($JAVA_HOME). Spark 4.x requires Java 17+."
  fi
else
  echo "   ↳ No JAVA_HOME set; will use system java (may be incompatible with Spark 4.x)"
fi

# Start platform services.
echo "1/5 ▶ Starting Kafka, Zookeeper, and Streamlit..."
$DOCKER_CMD compose up -d zookeeper kafka streamlit

if pgrep -f "docker.*logs.*streamlit-dashboard\|docker.*logs.*streamlit" >/dev/null 2>&1; then
  echo "   ↳ Streamlit log collector already running, skipping"
else
  nohup $DOCKER_CMD logs -f streamlit-dashboard > "$STREAMLIT_LOG" 2>&1 &
  echo "   ↳ streamlit log collector: $STREAMLIT_LOG"
fi

echo "2/5 ▶ Creating Kafka topics..."
$DOCKER_CMD exec kafka kafka-topics --create --topic "$INPUT_TOPIC" \
  --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 --if-not-exists >/dev/null
$DOCKER_CMD exec kafka kafka-topics --create --topic "$OUTPUT_TOPIC" \
  --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 --if-not-exists >/dev/null

echo "3/5 ▶ Starting streaming scorer..."
if pgrep -f "app.streaming_spark" >/dev/null 2>&1; then
    echo "   ↳ streaming_spark already running, skipping"
else
    STREAM_LOG="$LOG_DIR/streaming_realtime.log"
    if [[ -n "${SPARK_HOME:-}" && -x "${SPARK_HOME}/bin/spark-submit" ]]; then
        echo "   ↳ launching with spark-submit from $SPARK_HOME"
        if [[ -x "$PROJECT_ROOT/.venv/bin/python" ]]; then
          PYSPARK_PYTHON="$PROJECT_ROOT/.venv/bin/python"
        else
          PYSPARK_PYTHON="$(which python3 || echo python3)"
        fi
        export PYSPARK_PYTHON
        export PYSPARK_DRIVER_PYTHON="$PYSPARK_PYTHON"
        # Launch via helper script which logs env and optionally runs as the original user
        nohup "$PROJECT_ROOT/scripts/spark_launch.sh" > "$STREAM_LOG" 2>&1 &
    else
        echo "   ↳ launching with python (ensure .venv installed)"
        PY_BIN="$PROJECT_ROOT/.venv/bin/python"
        if [[ -x "$PY_BIN" ]]; then
            nohup env PYTHONPATH="$PYTHONPATH" "$PY_BIN" -m app.streaming_spark \
              --kafka-servers "$KAFKA_BROKER" \
              --input-topic "$INPUT_TOPIC" \
              --output-topic "$OUTPUT_TOPIC" \
              --model-path "$MODEL_PATH" \
              --checkpoint-dir "$CHECKPOINT_DIR" \
              --checkpoint-policy unique \
              > "$STREAM_LOG" 2>&1 &
        else
            nohup env PYTHONPATH="$PROJECT_ROOT" python3 -m app.streaming_spark \
              --kafka-servers "$KAFKA_BROKER" \
              --input-topic "$INPUT_TOPIC" \
              --output-topic "$OUTPUT_TOPIC" \
              --model-path "$MODEL_PATH" \
              --checkpoint-dir "$CHECKPOINT_DIR" \
              --checkpoint-policy unique \
              > "$STREAM_LOG" 2>&1 &
        fi
    fi
    echo "   ↳ log: $STREAM_LOG"
fi

echo "4/5 ▶ Starting producer..."
if pgrep -f "app.producer_youtube" >/dev/null 2>&1; then
  echo "   ↳ producer_youtube already running, skipping"
else
  PRODUCER_LOG="$LOG_DIR/producer_real.log"
  PY_BIN="$PROJECT_ROOT/.venv/bin/python"

  # Build the start command ensuring unbuffered Python output
  if [[ -x "$PY_BIN" ]]; then
    START_CMD="env PYTHONUNBUFFERED=1 PYTHONPATH=\"$PYTHONPATH\" \"$PY_BIN\" -u -m app.producer_youtube \
      --kafka-servers \"$KAFKA_BROKER\" \
      --topic \"$INPUT_TOPIC\" \
      --region-code \"$REGION_CODE\" \
      --max-results \"$MAX_RESULTS\" \
      --poll-interval \"$POLL_INTERVAL\""
  else
    START_CMD="env PYTHONUNBUFFERED=1 PYTHONPATH=\"$PROJECT_ROOT\" python3 -u -m app.producer_youtube \
      --kafka-servers \"$KAFKA_BROKER\" \
      --topic \"$INPUT_TOPIC\" \
      --region-code \"$REGION_CODE\" \
      --max-results \"$MAX_RESULTS\" \
      --poll-interval \"$POLL_INTERVAL\""
  fi

  # If the launcher was run under sudo, start the producer as the original user so it uses the user's venv/logs
  if [[ -n "${SUDO_USER:-}" && "$(id -u)" -eq 0 ]]; then
    nohup sudo -u "$SUDO_USER" bash -lc "$START_CMD" > "$PRODUCER_LOG" 2>&1 &
  else
    nohup bash -lc "$START_CMD" > "$PRODUCER_LOG" 2>&1 &
  fi

  echo "   ↳ log: $PRODUCER_LOG"
fi

echo "5/5 ▶ Done"
echo ""
echo "Open Streamlit: http://localhost:8501"
echo "Watch producer log: tail -f \"$LOG_DIR/producer_real.log\""
echo "Watch streaming log: tail -f \"$LOG_DIR/streaming_realtime.log\""
echo "View realtime consumer (optional): ./consumer_wrapper.sh"
