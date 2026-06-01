#!/bin/bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="$PROJECT_ROOT/logs"
mkdir -p "$LOG_DIR"

STAMP="$(date +%Y%m%d_%H%M%S)"
LOG_FILE="$LOG_DIR/train_models_resume_${STAMP}.log"

models=(
  linear_regression
  gradient_boosting
  random_forest
)

echo "🚀 Starting resumable per-model training"
echo "   Project root: $PROJECT_ROOT"
echo "   Log file: $LOG_FILE"

nohup env PROJECT_ROOT="$PROJECT_ROOT" PYTHONPATH="$PROJECT_ROOT" bash -s -- "$@" > "$LOG_FILE" 2>&1 <<'EOF' &
set -euo pipefail

cd "$PROJECT_ROOT"

run_model() {
  local model_name="$1"
  local model_dir="models/$model_name"
  local metrics_file="models/${model_name}_metrics.json"

  if [[ -f "$metrics_file" ]]; then
    echo "Skipping $model_name, metrics already exist"
    return 0
  fi

  echo "Training $model_name"
  python3 -m app.train_spark \
    --model "$model_name" \
    --save-model "$model_dir" \
    --model-output-dir models \
    --no-tune
}

for model_name in "${models[@]}"; do
  if ! run_model "$model_name"; then
    echo "Model $model_name failed, continuing to the next one"
  fi
done

python3 -m app.ml.aggregate_training_metrics \
  --model-output-dir models \
  --metrics-output metrics/regression_metrics.json \
  --best-model-link models/rf_regression_model

python3 -m app.predict_spark \
  --model-path models/rf_regression_model \
  --data data/cleaned_youtube_regression.parquet
EOF

PIPELINE_PID=$!
echo "   Background PID: $PIPELINE_PID"
echo "   Check progress: tail -f \"$LOG_FILE\""