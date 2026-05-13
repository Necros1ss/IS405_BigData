#!/usr/bin/env python3
"""csv_to_parquet.py
Convert CSV to Parquet. Prefer running with pyspark for large files; falls back to pandas+pyarrow.
Usage:
  python3 csv_to_parquet.py input.csv output.parquet
"""
import sys
from pathlib import Path


def ensure_spark_runtime():
    import os

    spark_home = os.environ.get("SPARK_HOME", "/home/thinh/spark")
    bundled_paths = [
        os.path.join(spark_home, "python"),
        os.path.join(spark_home, "python", "lib", "pyspark.zip"),
        os.path.join(spark_home, "python", "lib", "py4j-src.zip"),
    ]

    for path in reversed(bundled_paths):
        if os.path.exists(path) and path not in sys.path:
            sys.path.insert(0, path)

def convert_with_pyspark(inp, out):
    ensure_spark_runtime()
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName("csv_to_parquet").getOrCreate()
    df = spark.read.option("header", True).csv(str(inp))
    df.write.parquet(str(out), mode="overwrite", compression="snappy")
    spark.stop()

def convert_with_pandas(inp, out):
    import pandas as pd
    df = pd.read_csv(inp)
    df.to_parquet(out, engine="pyarrow", compression="snappy")

def main():
    if len(sys.argv) < 3:
        print("Usage: python3 csv_to_parquet.py input.csv output.parquet")
        sys.exit(1)
    inp = Path(sys.argv[1])
    out = Path(sys.argv[2])
    if not inp.exists():
        print("Input not found:", inp)
        sys.exit(2)
    try:
        print("Trying pyspark conversion...")
        convert_with_pyspark(inp, out)
        print("Converted with pyspark ->", out)
    except Exception:
        print("pyspark failed or not available, falling back to pandas+pyarrow")
        convert_with_pandas(inp, out)
        print("Converted with pandas ->", out)

if __name__ == '__main__':
    main()
