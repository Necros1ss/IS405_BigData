#!/usr/bin/env bash
set -euo pipefail

# orchestrate_full_pipeline.sh
# Run on the VM as the project user (thinh). It will:
#  - check for Hadoop, install if missing (uses setup_hadoop_single_node.sh)
#  - ensure HDFS/YARN are running when available
#  - clean the raw CSV with Spark
#  - train the model from the cleaned parquet output
# Usage:
#   sudo bash app/scripts/orchestrate_full_pipeline.sh /home/thinh/data/trending_yt_videos_113_countries.csv

PROJECT_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
SETUP_SCRIPT="$PROJECT_ROOT/app/scripts/setup_hadoop_single_node.sh"
HADOOP_INSTALL="/opt/hadoop"

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <local-csv-path> [--hdfs-user thinh] [--num-trees 100] [--max-depth 12]"
  exit 1
fi

LOCAL_CSV="$1"
shift || true
HDFS_USER="thinh"
NUM_TREES=100
MAX_DEPTH=12

while [[ $# -gt 0 ]]; do
  case $1 in
    --hdfs-user) HDFS_USER="$2"; shift 2;;
    --num-trees) NUM_TREES="$2"; shift 2;;
    --max-depth) MAX_DEPTH="$2"; shift 2;;
    *) echo "Unknown arg $1"; exit 1;;
  esac
done

if [[ ! -f "$LOCAL_CSV" ]]; then
  echo "Local CSV not found: $LOCAL_CSV" >&2
  exit 2
fi

echo "Checking Hadoop installation..."
if ! command -v hdfs >/dev/null 2>&1; then
  echo "Hadoop not found; running setup script..."
  if [[ -f "$SETUP_SCRIPT" ]]; then
    sudo bash "$SETUP_SCRIPT"
  else
    echo "Setup script not found at $SETUP_SCRIPT" >&2
    exit 3
  fi
fi

echo "Starting HDFS/YARN if not running..."
if ! jps | grep -q NameNode; then
  ${HADOOP_INSTALL}/sbin/start-dfs.sh || true
fi
if ! jps | grep -q ResourceManager; then
  ${HADOOP_INSTALL}/sbin/start-yarn.sh || true
fi

echo "Cleaning raw CSV into parquet..."
RAW_DATA_PATH="${LOCAL_CSV}" python3 -m app.spark_data_cleaner

echo "Running Spark training pipeline on cleaned parquet..."
cd "$PROJECT_ROOT"
bash scripts/run_spark.sh "data/cleaned_youtube_regression.parquet" --num-trees ${NUM_TREES} --max-depth ${MAX_DEPTH} --save-model "models/rf_regression_model"
echo "Pipeline finished. Cleaned data saved to data/cleaned_youtube_regression.parquet and model saved to models/rf_regression_model."
