#!/bin/bash

echo "🔄 Producer Loop - Continuous YouTube Data Streaming"

PROJECT_ROOT="$(cd "$(dirname "$0")" && pwd)"
ENV_FILE="$PROJECT_ROOT/.env"

cd "$PROJECT_ROOT"

if [[ -f "$ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
fi

while true; do
  echo ""
  echo "[$(date '+%H:%M:%S')] Batch started..."

  if [[ -z "${YOUTUBE_API_KEY:-}" ]]; then
    echo "✗ YOUTUBE_API_KEY is not set. The real trending producer cannot run without it."
    exit 1
  fi
  
  python3 -m app.producer_youtube \
    --kafka-servers localhost:9092 \
    --topic youtube_videos \
    --region-code VN \
    --max-results 50 \
    --poll-interval 30 \
    --num-messages 50
  
  echo "[$(date '+%H:%M:%S')] ✓ Batch completed. Waiting 30s before next batch..."
  sleep 30
done
