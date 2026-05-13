#!/usr/bin/env bash
set -euo pipefail

KAFKA_HOME="${KAFKA_HOME:-$HOME/kafka}"
SERVER_CONFIG="${SERVER_CONFIG:-$KAFKA_HOME/config/server.properties}"
ZK_CONFIG="${ZK_CONFIG:-$KAFKA_HOME/config/zookeeper.properties}"
DRY_RUN=0
CLEAN_ONLY=0

usage() {
  cat <<'EOF'
Usage: reset_kafka.sh [options]

Reset Kafka + ZooKeeper metadata to prevent InconsistentClusterIdException.

Options:
  --kafka-home <path>     Kafka home directory (default: ~/kafka)
  --server-config <path>  server.properties path
  --zk-config <path>      zookeeper.properties path
  --clean-only            Stop + clean only, do not restart services
  --dry-run               Print actions without executing
  -h, --help              Show this help

Environment overrides:
  KAFKA_HOME, SERVER_CONFIG, ZK_CONFIG
EOF
}

log() {
  printf "[reset-kafka] %s\n" "$*"
}

run() {
  if [[ "$DRY_RUN" == "1" ]]; then
    log "DRY-RUN: $*"
  else
    eval "$@"
  fi
}

require_file() {
  local f="$1"
  if [[ ! -f "$f" ]]; then
    log "Missing config file: $f"
    exit 1
  fi
}

read_prop() {
  local file="$1"
  local key="$2"
  awk -F'=' -v key="$key" '
    $1 ~ /^[[:space:]]*#/ { next }
    $1 ~ /^[[:space:]]*$/ { next }
    {
      k=$1
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", k)
      if (k == key) {
        v=$2
        sub(/^[[:space:]]+/, "", v)
        sub(/[[:space:]]+$/, "", v)
        print v
        exit
      }
    }
  ' "$file"
}

wait_port() {
  local port="$1"
  local name="$2"
  local max_tries=20
  local i=1

  while (( i <= max_tries )); do
    if timeout 1 bash -c "</dev/tcp/127.0.0.1/$port" >/dev/null 2>&1; then
      log "$name is ready on port $port"
      return 0
    fi
    sleep 1
    ((i++))
  done

  log "Timeout waiting for $name on port $port"
  return 1
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --kafka-home)
      KAFKA_HOME="$2"
      shift 2
      ;;
    --server-config)
      SERVER_CONFIG="$2"
      shift 2
      ;;
    --zk-config)
      ZK_CONFIG="$2"
      shift 2
      ;;
    --clean-only)
      CLEAN_ONLY=1
      shift
      ;;
    --dry-run)
      DRY_RUN=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      log "Unknown argument: $1"
      usage
      exit 1
      ;;
  esac
done

require_file "$SERVER_CONFIG"
require_file "$ZK_CONFIG"

LOG_DIRS_RAW="$(read_prop "$SERVER_CONFIG" "log.dirs")"
ZK_DATA_DIR="$(read_prop "$ZK_CONFIG" "dataDir")"

if [[ -z "$LOG_DIRS_RAW" ]]; then
  LOG_DIRS_RAW="/tmp/kafka-logs"
fi

if [[ -z "$ZK_DATA_DIR" ]]; then
  ZK_DATA_DIR="/tmp/zookeeper"
fi

IFS=',' read -r -a LOG_DIRS <<< "$LOG_DIRS_RAW"

log "Kafka home      : $KAFKA_HOME"
log "Server config   : $SERVER_CONFIG"
log "ZK config       : $ZK_CONFIG"
log "Kafka log dirs  : ${LOG_DIRS[*]}"
log "ZK data dir     : $ZK_DATA_DIR"

log "Stopping Kafka/ZooKeeper processes"
run "pkill -f 'kafka.Kafka' || true"
run "pkill -f 'QuorumPeerMain' || true"
run "pkill -f 'zookeeper' || true"

log "Cleaning Kafka metadata directories"
for d in "${LOG_DIRS[@]}"; do
  d="${d// /}"
  [[ -z "$d" ]] && continue
  run "rm -rf '$d'"
  run "mkdir -p '$d'"
done

log "Cleaning ZooKeeper data directory"
run "rm -rf '$ZK_DATA_DIR'"
run "mkdir -p '$ZK_DATA_DIR'"

if [[ "$CLEAN_ONLY" == "1" ]]; then
  log "Clean-only mode finished"
  exit 0
fi

log "Starting ZooKeeper"
run "cd '$KAFKA_HOME' && nohup bin/zookeeper-server-start.sh '$ZK_CONFIG' >/tmp/zookeeper-reset.log 2>&1 &"

if [[ "$DRY_RUN" == "0" ]]; then
  wait_port 2181 "ZooKeeper"
fi

log "Starting Kafka broker"
run "cd '$KAFKA_HOME' && nohup bin/kafka-server-start.sh '$SERVER_CONFIG' >/tmp/kafka-reset.log 2>&1 &"

if [[ "$DRY_RUN" == "0" ]]; then
  wait_port 9092 "Kafka broker"
  log "Kafka reset complete"
  log "Logs: /tmp/zookeeper-reset.log and /tmp/kafka-reset.log"
fi
