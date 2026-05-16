#!/usr/bin/env python3
"""Ensure Spark scripts use the bundled PySpark runtime.

The local environment has a Spark distribution under /home/thinh/spark that
matches the JVM runtime. Importing the pip-installed pyspark package first can
mix incompatible Python and JVM versions, so Spark entrypoints should call this
before importing pyspark.
"""

import os
import sys


def ensure_spark_runtime():
    spark_home = os.environ.get("SPARK_HOME", "/home/thinh/spark")
    java_home_candidates = [
        "/usr/lib/jvm/java-17-openjdk-amd64",
        "/usr/lib/jvm/java-1.17.0-openjdk-amd64",
        "/usr/lib/jvm/openjdk-17",
    ]

    for candidate in java_home_candidates:
        if os.path.exists(candidate):
            os.environ["JAVA_HOME"] = candidate
            os.environ["PATH"] = os.path.join(candidate, "bin") + os.pathsep + os.environ.get("PATH", "")
            break

    bundled_paths = [
        os.path.join(spark_home, "python"),
        os.path.join(spark_home, "python", "lib", "pyspark.zip"),
        os.path.join(spark_home, "python", "lib", "py4j-src.zip"),
    ]

    for path in reversed(bundled_paths):
        if os.path.exists(path) and path not in sys.path:
            sys.path.insert(0, path)

    os.environ.setdefault("SPARK_HOME", spark_home)
