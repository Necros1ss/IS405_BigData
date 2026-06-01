#!/bin/bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="$PROJECT_ROOT/logs"
mkdir -p "$LOG_DIR"

STAMP="$(date +%Y%m%d_%H%M%S)"
LOG_FILE="$LOG_DIR/train_predict_${STAMP}.log"

echo "🚀 Starting full pipeline in the background"
echo "   Project root: $PROJECT_ROOT"
echo "   Log file: $LOG_FILE"

nohup env PROJECT_ROOT="$PROJECT_ROOT" PYTHONPATH="$PROJECT_ROOT" bash -s -- "$@" > "$LOG_FILE" 2>&1 <<'EOF' &
set -euo pipefail

cd "$PROJECT_ROOT"

python3 -m app.train_spark --ultra-fast --no-tune "$@"

python3 -m app.predict_spark \
  --model-path models/rf_regression_model \
  --data data/cleaned_youtube_regression.parquet
EOF

PIPELINE_PID=$!
echo "   Background PID: $PIPELINE_PID"
echo "   Check progress: tail -f \"$LOG_FILE\""
