#!/bin/bash

echo "🔄 Producer Loop - Continuous YouTube Data Streaming"

while true; do
  echo ""
  echo "[$(date '+%H:%M:%S')] Batch started..."
  
  python3 -m app.producer_youtube \
    --kafka-servers localhost:9092 \
    --topic youtube_videos \
    --source youtube-api \
    --num-messages 20
  
  echo "[$(date '+%H:%M:%S')] ✓ Batch completed. Waiting 30s before next batch..."
  sleep 30
done
