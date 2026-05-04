"""
Data loading and feature engineering utilities for Spark.
"""
import os
import sys

def _append_local_spark_paths():
    spark_home = os.environ.get("SPARK_HOME")
    if not spark_home:
        for candidate in ("/home/thinh/spark", os.path.expanduser("~/spark")):
            if os.path.isdir(candidate):
                spark_home = candidate
                os.environ["SPARK_HOME"] = candidate
                break

    if not spark_home:
        return

    spark_python = os.path.join(spark_home, "python")
    spark_pyspark_zip = os.path.join(spark_home, "python", "lib", "pyspark.zip")
    spark_py4j = os.path.join(spark_home, "python", "lib", "py4j-0.10.9.9-src.zip")

    for path in (spark_python, spark_pyspark_zip, spark_py4j):
        if os.path.exists(path) and path not in sys.path:
            sys.path.insert(0, path)


try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import DoubleType
except Exception:
    _append_local_spark_paths()
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import DoubleType


def build_spark_session(app_name="YouTubeTrendingSpark"):
    return SparkSession.builder.appName(app_name).getOrCreate()


def load_csv_with_spark(spark, path_pattern):
    print(f"Loading CSVs matching: {path_pattern}")
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(path_pattern)
    print(f"Loaded {df.count()} rows and {len(df.columns)} columns")
    return df


def normalize_input_columns(df):
    rename_map = {
        "like_count": "likes",
        "video_tags": "tags",
    }

    for source_name, target_name in rename_map.items():
        if source_name in df.columns and target_name not in df.columns:
            df = df.withColumnRenamed(source_name, target_name)

    if "tags" not in df.columns:
        df = df.withColumn("tags", F.lit(None).cast("string"))

    if "likes" not in df.columns:
        df = df.withColumn("likes", F.lit(None).cast("double"))

    return df


def engineer_features(df):
    def safe_double(column_name, default_value):
        return F.coalesce(F.expr(f"try_cast(`{column_name}` as double)"), F.lit(default_value))

    def safe_ratio(numerator, denominator):
        return F.coalesce(F.expr(f"try_divide({numerator}, {denominator})"), F.lit(0.0))

    # Ensure numeric columns
    df = df.withColumn("view_count", safe_double("view_count", 1.0))
    df = df.withColumn("likes", safe_double("likes", 0.0))
    df = df.withColumn("comment_count", safe_double("comment_count", 0.0))

    # tag_count from tags column
    df = df.withColumn("tag_count", F.when(F.col("tags").isNull(), F.lit(0))
                       .otherwise(F.size(F.split(F.col("tags"), "\\|"))))

    # description_length
    df = df.withColumn("description_length", F.when(F.col("description").isNull(), F.lit(0))
                       .otherwise(F.length(F.col("description"))))

    # ratios
    df = df.withColumn("like_ratio", safe_ratio("likes", "view_count + 1.0"))
    df = df.withColumn("comment_ratio", safe_ratio("comment_count", "view_count + 1.0"))

    # engagement = likes + comments
    df = df.withColumn("engagement", (F.col("likes") + F.col("comment_count")).cast(DoubleType()))

    # Drop rows with nulls in key features
    df = df.dropna(subset=["tag_count", "description_length", "like_ratio", "comment_ratio", "engagement"])

    return df.select("tag_count", "description_length", "like_ratio", "comment_ratio", "engagement")
