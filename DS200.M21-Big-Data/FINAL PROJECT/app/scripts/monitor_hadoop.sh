#!/usr/bin/env bash
set -euo pipefail

# monitor_hadoop.sh - quick checks for Hadoop on local machine

echo "Processes (jps):"
if command -v jps >/dev/null 2>&1; then
  jps || true
else
  echo "jps not found; ensure JDK is installed and PATH includes jps"
fi

if command -v hdfs >/dev/null 2>&1; then
  echo
  echo "HDFS report:"
  hdfs dfsadmin -report || true
  echo
  echo "HDFS root listing:"
  hdfs dfs -ls / || true
else
  echo "hdfs binary not found in PATH"
fi

if command -v yarn >/dev/null 2>&1; then
  echo
  echo "YARN nodes:"
  yarn node -list || true
else
  echo "yarn binary not found in PATH"
fi

echo
echo "Web UIs: NameNode -> http://localhost:9870  ResourceManager -> http://localhost:8088"
