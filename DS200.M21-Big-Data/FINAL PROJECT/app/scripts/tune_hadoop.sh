#!/usr/bin/env bash
set -euo pipefail

# tune_hadoop.sh
# Usage: sudo bash tune_hadoop.sh [--hadoop-home /opt/hadoop] [--mem 3072] [--vcores 2] [--map-mb 1024] [--reduce-mb 2048] [--blocksize 134217728] [--replication 1]
# This script updates core Hadoop site files with sensible defaults for a single-node VM.

HADOOP_HOME_DEFAULT="/opt/hadoop"
HADOOP_HOME=${HADOOP_HOME:-$HADOOP_HOME_DEFAULT}
CONF_DIR="${HADOOP_HOME}/etc/hadoop"

MEM=3072
VCORES=2
MAP_MB=1024
REDUCE_MB=2048
BLOCKSIZE=134217728
REPLICATION=1

while [[ $# -gt 0 ]]; do
  case $1 in
    --hadoop-home) HADOOP_HOME="$2"; CONF_DIR="$HADOOP_HOME/etc/hadoop"; shift 2;;
    --mem) MEM="$2"; shift 2;;
    --vcores) VCORES="$2"; shift 2;;
    --map-mb) MAP_MB="$2"; shift 2;;
    --reduce-mb) REDUCE_MB="$2"; shift 2;;
    --blocksize) BLOCKSIZE="$2"; shift 2;;
    --replication) REPLICATION="$2"; shift 2;;
    *) echo "Unknown arg: $1"; exit 1;;
  esac
done

if [[ ! -d "$CONF_DIR" ]]; then
  echo "Hadoop conf dir not found: $CONF_DIR" >&2
  exit 2
fi

BACKUP_DIR="${CONF_DIR}/backup_$(date +%s)"
mkdir -p "$BACKUP_DIR"
cp ${CONF_DIR}/*.xml "$BACKUP_DIR/" || true
echo "Backed up xml files to $BACKUP_DIR"

add_or_replace_prop() {
  local file="$1" name="$2" value="$3"
  # remove existing property with same name
  perl -0777 -pe "s{<property>.*?<name>\Q${name}\E.*?</property>}{}gs" "$file" > "${file}.tmp"
  mv "${file}.tmp" "$file"
  # insert new property before </configuration>
  awk -v n="$name" -v v="$value" 'BEGIN{prop="  <property>\n    <name>"n"</name>\n    <value>"v"</value>\n  </property>\n"} {if($0 ~ /<\/configuration>/ && !x){print prop; x=1} print $0}' "$file" > "${file}.new"
  mv "${file}.new" "$file"
}

echo "Tuning Hadoop configs in $CONF_DIR"

# core-site.xml: ensure fs.defaultFS exists (leave as is if present)
if ! grep -q "fs.defaultFS" ${CONF_DIR}/core-site.xml; then
  add_or_replace_prop ${CONF_DIR}/core-site.xml fs.defaultFS "hdfs://localhost:9000"
fi

# hdfs-site.xml
add_or_replace_prop ${CONF_DIR}/hdfs-site.xml dfs.replication ${REPLICATION}
add_or_replace_prop ${CONF_DIR}/hdfs-site.xml dfs.blocksize ${BLOCKSIZE}

# yarn-site.xml
add_or_replace_prop ${CONF_DIR}/yarn-site.xml yarn.nodemanager.resource.memory-mb ${MEM}
add_or_replace_prop ${CONF_DIR}/yarn-site.xml yarn.nodemanager.resource.cpu-vcores ${VCORES}

# mapred-site.xml
add_or_replace_prop ${CONF_DIR}/mapred-site.xml mapreduce.map.memory.mb ${MAP_MB}
add_or_replace_prop ${CONF_DIR}/mapred-site.xml mapreduce.reduce.memory.mb ${REDUCE_MB}
add_or_replace_prop ${CONF_DIR}/mapred-site.xml mapreduce.map.java.opts "-Xmx$((MAP_MB*80/100))m"
add_or_replace_prop ${CONF_DIR}/mapred-site.xml mapreduce.reduce.java.opts "-Xmx$((REDUCE_MB*80/100))m"

echo "Tuning complete. Review files in $CONF_DIR and restart Hadoop services (stop/start-dfs.sh and stop/start-yarn.sh)."
