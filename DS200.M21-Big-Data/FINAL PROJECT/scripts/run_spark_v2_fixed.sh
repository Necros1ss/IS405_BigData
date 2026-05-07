#!/bin/bash
#
# Run YouTube Trending Pipeline V2 (Fixed - No Data Leakage)
# Usage: bash scripts/run_spark_v2_fixed.sh [options]
#

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_DIR="$( dirname "$SCRIPT_DIR" )"
cd "$PROJECT_DIR"

# Default values
DATA_PATH="${1:-kaggle_youtube/trending_yt_videos_113_countries.csv}"
NO_SAMPLE="${2:---no-sample}"
# Use smaller defaults to reduce memory usage and avoid OOM
NUM_TREES="${3:-20}"
MAX_DEPTH="${4:-8}"
SAVE_MODEL="${5:-/home/thinh/rf_model_v2_fixed}"

# Logging
LOG_FILE="/tmp/train_v2_fixed_run.log"
exec > >(tee -a "$LOG_FILE")
exec 2>&1

echo "=========================================="
echo "YouTube Trending Pipeline V2 (FIXED)"
echo "=========================================="
echo "Data: $DATA_PATH"
echo "Num Trees: $NUM_TREES"
echo "Max Depth: $MAX_DEPTH"
echo "Save Model: $SAVE_MODEL"
echo "Log File: $LOG_FILE"
echo ""

# Setup environment
export SPARK_HOME=/home/thinh/spark
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3
export PYTHONPATH="${PROJECT_DIR}:${PYTHONPATH}"
export SPARK_LOCAL_DIRS=/home/thinh/spark_local

# Tune PySpark submit args to reduce memory pressure (avoid OOM)
# - lower shuffle partitions, cap driver result size, provide reasonable driver/executor memory
export PYSPARK_SUBMIT_ARGS="--conf spark.sql.shuffle.partitions=50 \
    --conf spark.driver.memory=6g \
    --conf spark.executor.memory=6g \
    --conf spark.driver.maxResultSize=1g \
    --conf spark.memory.fraction=0.6 \
    --conf spark.memory.storageFraction=0.2 \
    pyspark-shell"

echo "PYSPARK_SUBMIT_ARGS=$PYSPARK_SUBMIT_ARGS"

echo "[Environment]"
echo "  SPARK_HOME=$SPARK_HOME"
echo "  JAVA_HOME=$JAVA_HOME"
echo "  PYTHONPATH=$PYTHONPATH"
echo ""

# Activate Python venv if available
if [ -f "$HOME/venv_spark/bin/activate" ]; then
    source "$HOME/venv_spark/bin/activate"
    echo "✓ Activated Python venv"
fi

# Run training
echo "[Starting Training...]"
python3 -m app.app_spark_v2_fixed \
    --data "$DATA_PATH" \
    $NO_SAMPLE \
    --num-trees "$NUM_TREES" \
    --max-depth "$MAX_DEPTH" \
    --save-model "$SAVE_MODEL" \
    --save-metrics "${PROJECT_DIR}/Images/metrics_v2_fixed.json"

echo ""
echo "=========================================="
echo "✓ Training Completed"
echo "=========================================="
echo "Model saved to: $SAVE_MODEL"
echo "Log saved to: $LOG_FILE"
