#!/bin/bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="$PROJECT_ROOT/logs"
mkdir -p "$LOG_DIR"

DOCKER_CMD="docker"
if ! docker ps >/dev/null 2>&1; then
    if sudo -n docker ps >/dev/null 2>&1; then
        DOCKER_CMD="sudo docker"
    fi
fi

SHOW_HELP=0
STOP_DOCKER=0
DOWN_DOCKER=0

for arg in "$@"; do
  case "$arg" in
    --docker) STOP_DOCKER=1 ;;
    --down) DOWN_DOCKER=1 ;;
    -h|--help) SHOW_HELP=1 ;;
    *) ;;
  esac
done

if [[ $SHOW_HELP -eq 1 ]]; then
  cat <<'USAGE'
Usage: ./stop_realtime.sh [--docker] [--down]

Stops the realtime processes started by run_realtime.sh:
 - kills local python producers and streaming job
 - stops the streamlit container log follower
Options:
 --docker   : also stops Kafka, Zookeeper and Streamlit containers (docker compose stop)
 --down     : stops and removes containers (docker compose down)
USAGE
  exit 0
fi

echo "⏹️  Stopping realtime processes..."

# Kill producer
if pgrep -f "app.producer_youtube" >/dev/null 2>&1; then
  echo " - Stopping producer_youtube"
  pkill -f "app.producer_youtube" || true
else
  echo " - producer_youtube not running"
fi

# Kill streaming_spark
if pgrep -f "app.streaming_spark" >/dev/null 2>&1; then
  echo " - Stopping streaming_spark"
  pkill -f "app.streaming_spark" || true
else
  echo " - streaming_spark not running"
fi

# Kill any docker logs -f streamlit-dashboard follower started by run_realtime
if pgrep -f "docker logs -f streamlit-dashboard" >/dev/null 2>&1 || pgrep -f "streamlit_container_" >/dev/null 2>&1; then
  echo " - Stopping streamlit log follower"
  pkill -f "docker logs -f streamlit-dashboard" || true
fi

# Optionally stop Docker services
if [[ $STOP_DOCKER -eq 1 ]]; then
  echo "⏹️  Stopping Docker services (kafka, zookeeper, streamlit)"
  $DOCKER_CMD compose stop streamlit kafka zookeeper || true
fi

if [[ $DOWN_DOCKER -eq 1 ]]; then
  echo "🗑️  Bringing down containers (docker compose down)"
  $DOCKER_CMD compose down || true
fi

echo "✅ stop_realtime finished. Logs are in: $LOG_DIR"
