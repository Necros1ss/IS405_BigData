#!/bin/bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <linear_regression|gradient_boosting|random_forest|random_forest_tuned> [extra app.train_spark args...]"
  exit 1
fi

MODEL_NAME="$1"
shift

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_ROOT"

case "$MODEL_NAME" in
  linear_regression|gradient_boosting|random_forest|random_forest_tuned)
    ;;
  *)
    echo "Unsupported model: $MODEL_NAME"
    exit 1
    ;;
esac

python3 -m app.train_spark \
  --model "$MODEL_NAME" \
  --save-model "models/$MODEL_NAME" \
  --model-output-dir models \
  --no-tune "$@"