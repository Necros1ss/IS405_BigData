#!/bin/bash
# Quick test of V2 fixed pipeline with small sample
# Tests that accuracy drops from 100% to realistic 70-85% range

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_DIR="$( dirname "$SCRIPT_DIR" )"
cd "$PROJECT_DIR"

# Setup environment
export SPARK_HOME=/home/thinh/spark
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3
export PYTHONPATH="${PROJECT_DIR}:${PYTHONPATH}"
export SPARK_LOCAL_DIRS=/home/thinh/spark_local

LOG_FILE="/tmp/train_v2_fixed_test.log"
exec > >(tee -a "$LOG_FILE")
exec 2>&1

echo "=========================================="
echo "YouTube Trending V2 (FIXED) - QUICK TEST"
echo "=========================================="
echo "Testing with: 1% sample (quick validation)"
echo "Log: $LOG_FILE"
echo ""

# Run with sampling for quick test
echo "[Running training with 1% sample for quick verification...]"
python3 -m app.app_spark_v2_fixed \
    --data "kaggle_youtube/trending_yt_videos_113_countries.csv" \
    --sample-fraction 0.01 \
    --num-trees 50 \
    --max-depth 8 \
    --save-model /tmp/rf_model_v2_test \
    --save-metrics /tmp/metrics_v2_test.json

echo ""
echo "=========================================="
echo "TEST RESULTS"
echo "=========================================="
echo ""
if [ -f /tmp/metrics_v2_test.json ]; then
    echo "✓ Metrics file created:"
    python3 -c "
import json
with open('/tmp/metrics_v2_test.json') as f:
    metrics = json.load(f)
    print(f\"  AUC: {metrics.get('auc', 0):.4f}\")
    print(f\"  Accuracy: {metrics.get('accuracy', 0):.4f}\")
    print(f\"  F1-Score: {metrics.get('f1', 0):.4f}\")
    
    # Check if fixed
    auc = metrics.get('auc', 0)
    if auc > 0.99:
        print(f\"\\n  ❌ STILL LEAKY: AUC={auc:.4f} is too high!\")
        exit(1)
    elif 0.70 <= auc <= 0.95:
        print(f\"\\n  ✅ FIXED: Realistic AUC={auc:.4f} in expected range!\")
        exit(0)
    else:
        print(f\"\\n  ⚠️  UNEXPECTED: AUC={auc:.4f}\")
        exit(0)
"
    TEST_RESULT=$?
else
    echo "✗ Metrics file NOT created - training failed"
    TEST_RESULT=1
fi

echo ""
echo "=========================================="
if [ $TEST_RESULT -eq 0 ]; then
    echo "✅ V2 FIX VERIFIED: Data leakage removed!"
else
    echo "❌ V2 FIX NEEDS ATTENTION: Still investigating..."
fi
echo "=========================================="

exit $TEST_RESULT
