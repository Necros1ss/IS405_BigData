#!/usr/bin/env python3
"""Spark Structured Streaming job that scores live YouTube videos."""

import argparse
import os
import shutil
import time

from app.spark_bootstrap import ensure_spark_runtime

ensure_spark_runtime()

from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from app.utils.feature_engineering import add_shared_features


def get_streaming_schema():
    return StructType(
        [
            StructField("video_id", StringType(), True),
            StructField("title", StringType(), True),
            StructField("description", StringType(), True),
            StructField("publish_time", StringType(), True),
            StructField("tags", StringType(), True),
            StructField("views", DoubleType(), True),
            StructField("view_count", DoubleType(), True),
            StructField("likes", DoubleType(), True),
            StructField("comment_count", DoubleType(), True),
            StructField("country", StringType(), True),
            StructField("categoryId", StringType(), True),
        ]
    )


def resolve_checkpoint_dir(base_dir, policy):
    checkpoint_dir = os.path.abspath(base_dir)
    if policy == "unique":
        checkpoint_dir = os.path.join(checkpoint_dir, f"run_{int(time.time())}_{os.getpid()}")
    elif policy == "reset" and os.path.exists(checkpoint_dir):
        shutil.rmtree(checkpoint_dir)
    os.makedirs(checkpoint_dir, exist_ok=True)
    return checkpoint_dir


def run_stream():
    parser = argparse.ArgumentParser()
    parser.add_argument("--kafka-servers", default="localhost:9092")
    parser.add_argument("--input-topic", default="youtube_videos")
    parser.add_argument("--output-topic", default="youtube_predictions")
    parser.add_argument("--model-path", required=True)
    parser.add_argument("--checkpoint-dir", required=True)
    parser.add_argument(
        "--checkpoint-policy",
        choices=["keep", "unique", "reset"],
        default="keep",
        help="keep: reuse checkpoint-dir, unique: create a per-run subdir, reset: delete and recreate checkpoint-dir",
    )
    args = parser.parse_args()

    if not os.path.exists(args.model_path):
        raise FileNotFoundError(f"Model not found at {args.model_path}")

    kafka_package = os.getenv("SPARK_KAFKA_PACKAGE", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1")
    spark = (
        SparkSession.builder.appName("YouTubeTrendingRealtimePrediction")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.jars.packages", kafka_package)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    model = PipelineModel.load(args.model_path)

    stream_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.kafka_servers)
        .option("subscribe", args.input_topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed_df = (
        stream_df.select(F.from_json(F.col("value").cast("string"), get_streaming_schema()).alias("data"))
        .filter(F.col("data").isNotNull())
        .select("data.*")
        .filter(F.col("video_id").isNotNull() & (F.length(F.trim(F.col("video_id"))) > 0))
    )

    parsed_df = parsed_df.withColumn("parsed_trending_date", F.current_timestamp())
    parsed_df = parsed_df.withColumn("publish_timestamp", F.to_timestamp(F.col("publish_time")))

    features_df = add_shared_features(parsed_df, snapshot_ts_col="parsed_trending_date")
    predictions = model.transform(features_df)
    predictions = predictions.withColumn("predicted_trending_days", F.round(F.expr("expm1(raw_prediction)"), 2))

    output_df = predictions.select(
        F.col("video_id"),
        F.coalesce(F.col("title"), F.lit("Unknown Title")).alias("title"),
        F.coalesce(F.col("description"), F.lit("" )).alias("description"),
        F.coalesce(F.col("country"), F.lit("Unknown")).alias("country"),
        F.col("views_normalized").alias("views"),
        F.col("predicted_trending_days"),
        F.current_timestamp().alias("prediction_time"),
    )

    kafka_output_df = output_df.select(F.to_json(F.struct("*")).alias("value"))
    checkpoint_dir = resolve_checkpoint_dir(args.checkpoint_dir, args.checkpoint_policy)

    query = (
        kafka_output_df.writeStream.format("kafka")
        .option("kafka.bootstrap.servers", args.kafka_servers)
        .option("topic", args.output_topic)
        .option("checkpointLocation", checkpoint_dir)
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    run_stream()
