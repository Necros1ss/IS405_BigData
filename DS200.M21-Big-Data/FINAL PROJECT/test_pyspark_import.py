#!/usr/bin/env python3
"""Quick test to verify PySpark imports work."""

try:
    from pyspark.sql import SparkSession
    from pyspark.ml.feature import StringIndexer, VectorAssembler
    print("✓ PySpark imports OK")
except ImportError as e:
    print(f"✗ Import error: {e}")
    exit(1)

# Create a simple Spark session
spark = SparkSession.builder.appName("test").getOrCreate()
print(f"✓ SparkSession created: {spark.version}")
spark.stop()
