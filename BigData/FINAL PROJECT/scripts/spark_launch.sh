#!/bin/bash
# Helper to launch spark-submit while logging environment and preserving user's venv when run under sudo
set -euo pipefail

# Derive sensible defaults so the script is robust when invoked with a limited env
PROJECT_ROOT="${PROJECT_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
SPARK_HOME="${SPARK_HOME:-/home/thinh/spark}"
PYSPARK_PYTHON="${PYSPARK_PYTHON:-python3}"
PYSPARK_DRIVER_PYTHON="${PYSPARK_DRIVER_PYTHON:-$PYSPARK_PYTHON}"
JAVA_HOME="${JAVA_HOME:-}"

LOGFILE="$(mktemp /tmp/spark_launch_env.XXXXXX 2>/dev/null || echo /tmp/spark_launch_env.$$)"
trap 'rm -f "$LOGFILE"' EXIT
printf '%s\n' '--- spark-launch env ---' > "$LOGFILE"
env >> "$LOGFILE"

# Build spark-submit command (use explicit paths from variables)
CMD=("$SPARK_HOME/bin/spark-submit" --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 --master local[*] "$PROJECT_ROOT/app/streaming_spark.py" --kafka-servers "${KAFKA_BROKER:-localhost:9092}" --input-topic "${INPUT_TOPIC:-youtube_videos}" --output-topic "${OUTPUT_TOPIC:-youtube_predictions}" --model-path "${MODEL_PATH:-$PROJECT_ROOT/models/rf_regression_model}" --checkpoint-dir "${CHECKPOINT_DIR:-/tmp/spark_chkpt_youtube}" --checkpoint-policy unique)

# Ensure PYTHONPATH includes Spark python libs and the project so imports like `app` work
if [[ -d "$SPARK_HOME/python" ]]; then
  PY4J_ZIP=""
  for f in "$SPARK_HOME"/python/lib/py4j*.zip "$SPARK_HOME"/python/lib/py4j*.egg "$SPARK_HOME"/python/lib/py4j*src.zip; do
    [[ -e "$f" ]] && { PY4J_ZIP="$f"; break; }
  done
  if [[ -n "$PY4J_ZIP" ]]; then
    PYTHONPATH="$SPARK_HOME/python:$PY4J_ZIP:$PROJECT_ROOT${PYTHONPATH:+:$PYTHONPATH}"
  else
    PYTHONPATH="$SPARK_HOME/python:$PROJECT_ROOT${PYTHONPATH:+:$PYTHONPATH}"
  fi
else
  PYTHONPATH="$PROJECT_ROOT${PYTHONPATH:+:$PYTHONPATH}"
fi
export PYTHONPATH

# If running under sudo, run as the original user so their venv/site-packages are used
if [[ -n "${SUDO_USER:-}" ]]; then
  exec sudo -u "$SUDO_USER" env JAVA_HOME="$JAVA_HOME" PYSPARK_PYTHON="$PYSPARK_PYTHON" PYSPARK_DRIVER_PYTHON="$PYSPARK_DRIVER_PYTHON" PYTHONPATH="$PYTHONPATH" "${CMD[@]}"
else
  exec env JAVA_HOME="$JAVA_HOME" PYSPARK_PYTHON="$PYSPARK_PYTHON" PYSPARK_DRIVER_PYTHON="$PYSPARK_DRIVER_PYTHON" PYTHONPATH="$PYTHONPATH" "${CMD[@]}"
fi
