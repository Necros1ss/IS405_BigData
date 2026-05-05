#!/usr/bin/env python3
"""
Quick test script to verify streaming setup.
Checks if Kafka, PySpark, and other dependencies are available.

Usage:
    python3 test_streaming_setup.py
"""
import sys
import subprocess

def check_kafka():
    """Check if Kafka is installed and running."""
    print("\n📦 Checking Kafka...")
    
    # Check installation
    import os
    kafka_home = os.path.expanduser("~/kafka")
    if not os.path.exists(kafka_home):
        print("  ✗ Kafka not found at ~/kafka")
        print("    → Install from: https://kafka.apache.org/downloads")
        return False
    print(f"  ✓ Kafka found at {kafka_home}")
    
    # Check if running
    try:
        result = subprocess.run(
            ["lsof", "-i", ":9092"],
            capture_output=True,
            timeout=2
        )
        if result.returncode == 0:
            print("  ✓ Kafka broker is running on port 9092")
            return True
        else:
            print("  ⚠ Kafka broker not detected on port 9092")
            print("    → Start with: ~/kafka/bin/kafka-server-start.sh config/server.properties")
            return False
    except Exception:
        print("  ⚠ Could not check Kafka status (lsof not available)")
        return None

def check_dependencies():
    """Check Python dependencies."""
    print("\n📚 Checking Python dependencies...")
    
    deps = {
        "pyspark": "PySpark (core)",
        "kafka": "kafka-python (producer/consumer)",
        "matplotlib": "matplotlib (visualizations)"
    }
    
    all_ok = True
    for module, desc in deps.items():
        try:
            __import__(module)
            print(f"  ✓ {desc}")
        except ImportError:
            print(f"  ✗ {desc} - Install with: pip install {module}")
            all_ok = False
    
    return all_ok

def check_java():
    """Check Java installation."""
    print("\n☕ Checking Java...")
    
    try:
        result = subprocess.run(
            ["java", "-version"],
            capture_output=True,
            text=True,
            timeout=2
        )
        version_line = result.stderr.split('\n')[0]
        print(f"  ✓ Java found: {version_line}")
        return True
    except FileNotFoundError:
        print("  ✗ Java not found in PATH")
        print("    → Set JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64")
        return False

def check_spark():
    """Check Spark installation."""
    print("\n⚡ Checking Apache Spark...")
    
    import os
    spark_home = os.path.expanduser("~/spark")
    if not os.path.exists(spark_home):
        print("  ✗ Spark not found at ~/spark")
        return False
    
    print(f"  ✓ Spark found at {spark_home}")
    
    # Check PySpark
    try:
        from pyspark.sql import SparkSession
        print("  ✓ PySpark is importable")
        return True
    except ImportError:
        print("  ✗ PySpark not importable")
        return False

def check_files():
    """Check required files."""
    print("\n📄 Checking required files...")
    
    import os
    files = [
        "app/streaming_spark.py",
        "app/producer_youtube.py",
        "app/consumer_predictions.py",
        "app/app_spark.py",
        "app/clean_spark.py",
        "app/train_spark.py"
    ]
    
    all_ok = True
    for file in files:
        if os.path.exists(file):
            print(f"  ✓ {file}")
        else:
            print(f"  ✗ {file} - NOT FOUND")
            all_ok = False
    
    return all_ok

def main():
    """Run all checks."""
    print("=" * 60)
    print("🔍 Streaming Setup Verification")
    print("=" * 60)
    
    results = {}
    
    results["Java"] = check_java()
    results["Spark"] = check_spark()
    results["Kafka"] = check_kafka()
    results["Dependencies"] = check_dependencies()
    results["Files"] = check_files()
    
    # Summary
    print("\n" + "=" * 60)
    print("📋 SUMMARY")
    print("=" * 60)
    
    for component, status in results.items():
        if status is None:
            symbol = "⚠"
        elif status:
            symbol = "✓"
        else:
            symbol = "✗"
        print(f"{symbol} {component}")
    
    # Recommendations
    print("\n" + "=" * 60)
    print("🚀 NEXT STEPS")
    print("=" * 60)
    
    all_ready = all(v for v in results.values() if v is not None)
    
    if all_ready:
        print("""
✓ Your system is ready for streaming!

1. Start Kafka services (in different terminals):
   Terminal 1: ~/kafka/bin/zookeeper-server-start.sh config/zookeeper.properties
   Terminal 2: ~/kafka/bin/kafka-server-start.sh config/server.properties

2. Create Kafka topics:
   ~/kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --topic youtube_videos --partitions 1
   ~/kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --topic youtube_predictions --partitions 1

3. Run streaming pipeline:
   python3 -m app.streaming_spark --output console

4. In another terminal, run producer:
   python3 -m app.producer_youtube --rate 1

5. Optional: Monitor predictions:
   python3 -m app.consumer_predictions

📖 See STREAMING_GUIDE.md for detailed instructions!
        """)
    else:
        print("""
⚠ Some components need attention. Please:

1. Install missing dependencies
2. Check that Kafka is installed at ~/kafka
3. Set JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

Then re-run this check.
        """)
    
    return all_ready

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
