#!/usr/bin/env bash
# Run the Spark app with local SPARK_HOME and Java 17 configured
set -euo pipefail

export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
export SPARK_HOME=/home/thinh/spark
export SPARK_LOCAL_HOSTNAME=localhost
PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
export PYTHONPATH="$PROJECT_ROOT:$SPARK_HOME/python:$SPARK_HOME/python/lib/pyspark.zip:$SPARK_HOME/python/lib/py4j-0.10.9.9-src.zip"

cd "$PROJECT_ROOT"

# Activate a project virtualenv if present
if [ -f .venv/bin/activate ]; then
  # shellcheck source=/dev/null
  source .venv/bin/activate
elif [ -f .venv_spark/bin/activate ]; then
  # shellcheck source=/dev/null
  source .venv_spark/bin/activate
fi

# First positional argument is the data path (optional); remaining args are passed through
DATA_PATH="${1:-kaggle_youtube/trending_yt_videos_113_countries.csv}"
if [ "$#" -ge 1 ]; then
  # shift only if a positional was provided so $@ contains only flags
  shift
fi

# Ensure JAVA_HOME points to a Java 17 runtime used by Spark
if [ -z "$JAVA_HOME" ]; then
  if [ -d "/usr/lib/jvm/java-17-openjdk-amd64" ]; then
    export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
    export PATH="$JAVA_HOME/bin:$PATH"
  else
    echo "Warning: JAVA_HOME not set and Java 17 not found at /usr/lib/jvm/java-17-openjdk-amd64.\nPlease set JAVA_HOME to a Java 17 installation to avoid Spark/Hadoop runtime errors (getSubject is not supported)."
  fi
fi

# Default SPARK_HOME/PYTHONPATH if not set
if [ -z "$SPARK_HOME" ] && [ -d "/home/thinh/spark" ]; then
  export SPARK_HOME=/home/thinh/spark
fi
export SPARK_LOCAL_HOSTNAME=${SPARK_LOCAL_HOSTNAME:-localhost}
if [ -d "$SPARK_HOME" ]; then
  export PYTHONPATH="$PROJECT_ROOT:$SPARK_HOME/python:$SPARK_HOME/python/lib/pyspark.zip:$SPARK_HOME/python/lib/py4j-0.10.9.9-src.zip"
fi

python3 -m app.app_spark --data "$DATA_PATH" "$@"
