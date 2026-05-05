#!/usr/bin/env bash
set -euo pipefail

# orchestrate_full_pipeline.sh
# Run on the VM as the project user (thinh). It will:
#  - check for Hadoop, install if missing (uses setup_hadoop_single_node.sh)
#  - ensure HDFS/YARN are running
#  - put raw CSV from local path into HDFS
#  - run Spark pipeline reading from HDFS and save model/predictions back to HDFS
# Usage:
#   sudo bash app/scripts/orchestrate_full_pipeline.sh /home/thinh/data/trending_yt_videos_113_countries.csv

PROJECT_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
SETUP_SCRIPT="$PROJECT_ROOT/app/scripts/setup_hadoop_single_node.sh"
HADOOP_INSTALL="/opt/hadoop"
HADOOP_HOME="$HADOOP_INSTALL"
export HADOOP_HOME
export PATH="$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH"

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
  ${HADOOP_HOME}/sbin/start-dfs.sh || true
fi
if ! jps | grep -q ResourceManager; then
  ${HADOOP_HOME}/sbin/start-yarn.sh || true
fi

HDFS_INPUT_DIR="/user/${HDFS_USER}/input"
HDFS_MODEL_DIR="/user/${HDFS_USER}/models/rf"
HDFS_PRED_DIR="/user/${HDFS_USER}/predictions/rf"

echo "Putting local CSV into HDFS..."
sudo -u hadoop ${HADOOP_HOME}/bin/hdfs dfs -mkdir -p "/user/${HDFS_USER}"
sudo -u hadoop ${HADOOP_HOME}/bin/hdfs dfs -chown "${HDFS_USER}:supergroup" "/user/${HDFS_USER}"
sudo -u "${HDFS_USER}" ${HADOOP_HOME}/bin/hdfs dfs -mkdir -p "${HDFS_INPUT_DIR}"
sudo -u "${HDFS_USER}" ${HADOOP_HOME}/bin/hdfs dfs -put -f "${LOCAL_CSV}" "${HDFS_INPUT_DIR}/"

HDFS_CSV_PATH="hdfs://localhost:9000${HDFS_INPUT_DIR}/*.csv"

echo "Running Spark pipeline (reads from ${HDFS_CSV_PATH})"
cd "$PROJECT_ROOT"
bash scripts/run_spark.sh "${HDFS_CSV_PATH}" --no-sample --num-trees ${NUM_TREES} --max-depth ${MAX_DEPTH} --save-model ${HDFS_MODEL_DIR} --save-predictions ${HDFS_PRED_DIR} --save-metrics /tmp/rf_metrics.json
echo "Pipeline finished. Model saved to ${HDFS_MODEL_DIR}, predictions to ${HDFS_PRED_DIR} (HDFS)."
