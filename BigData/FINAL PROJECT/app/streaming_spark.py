#!/usr/bin/env python3

import os
import argparse
import shutil
import time

from app.spark_bootstrap import ensure_spark_runtime

ensure_spark_runtime()

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.ml import PipelineModel

# ============================================================
# SCHEMA & FEATURE ENGINEERING ĐỘC LẬP CHO STREAMING
# ============================================================
def get_streaming_schema():
    return StructType([
        StructField("video_id", StringType(), True),
        StructField("title", StringType(), True),
        StructField("publish_time", StringType(), True), 
        StructField("tags", StringType(), True),
        StructField("views", DoubleType(), True),
        StructField("view_count", DoubleType(), True),
        StructField("likes", DoubleType(), True),
        StructField("comment_count", DoubleType(), True),
        StructField("country", StringType(), True),
        StructField("categoryId", StringType(), True)
    ])

def apply_streaming_features(df):
    df = df.withColumn("views_normalized", F.coalesce(F.col("views"), F.col("view_count")))
    df = df.withColumn("categoryId", F.when(F.col("categoryId").isNull(), F.lit("0")).otherwise(F.col("categoryId")))
    df = df.withColumn("country", F.when(F.col("country").isNull(), F.lit("Unknown")).otherwise(F.col("country")))
    # Parse thời gian
    # Empty strings or malformed timestamps can appear in streaming payloads,
    # so normalize them to NULL before parsing.
    df = df.withColumn(
        "publish_timestamp",
        F.to_timestamp(
            F.when(F.trim(F.col("publish_time")) == "", None).otherwise(F.col("publish_time"))
        )
    )
    
    # Tính giờ trụ top trending (Mô phỏng: thời điểm hiện tại - thời điểm publish)
    # Fallback: if parsed_trending_date missing, use current_timestamp
    df = df.withColumn("parsed_trending_date", 
                       F.when(F.col("parsed_trending_date").isNotNull(), F.col("parsed_trending_date"))
                       .otherwise(F.current_timestamp()))
    
    df = df.withColumn(
        "hours_to_trending",
        (F.unix_timestamp(F.col("parsed_trending_date")) - F.unix_timestamp(F.col("publish_timestamp"))) / 3600.0
    )
    df = df.withColumn("hours_to_trending", F.when(F.col("hours_to_trending") < 0, 0.0).otherwise(F.col("hours_to_trending")))
    df = df.withColumn("log_hours_to_trending", F.log1p(F.col("hours_to_trending")))

    # Đặc trưng Text và Thời gian
    df = df.withColumn("title_length", F.when(F.col("title").isNull(), 0.0).otherwise(F.length(F.col("title")).cast(DoubleType())))
    df = df.withColumn("publish_hour", F.when(F.col("publish_timestamp").isNull(), 12.0).otherwise(F.hour(F.col("publish_timestamp")).cast(DoubleType())))
    df = df.withColumn("publish_dow", F.when(F.col("publish_timestamp").isNull(), 1.0).otherwise(F.dayofweek(F.col("publish_timestamp")).cast(DoubleType())))

    # Log biến đếm
    df = df.withColumn("log_views", F.log1p(F.col("views_normalized")))
    df = df.withColumn("log_likes", F.log1p(F.col("likes")))
    df = df.withColumn("log_comment_count", F.log1p(F.col("comment_count")))

    # Tính tỷ lệ
    df = df.withColumn("like_ratio", F.when(F.col("views_normalized") == 0, 0.0).otherwise(F.col("likes") / F.col("views_normalized")))
    df = df.withColumn("comment_ratio", F.when(F.col("views_normalized") == 0, 0.0).otherwise(F.col("comment_count") / F.col("views_normalized")))

    return df


def resolve_checkpoint_dir(base_dir, policy):
    checkpoint_dir = os.path.abspath(base_dir)

    if policy == "unique":
        run_suffix = f"run_{int(time.time())}_{os.getpid()}"
        checkpoint_dir = os.path.join(checkpoint_dir, run_suffix)
    elif policy == "reset" and os.path.exists(checkpoint_dir):
        shutil.rmtree(checkpoint_dir)

    os.makedirs(checkpoint_dir, exist_ok=True)
    return checkpoint_dir

# ============================================================
# MAIN STREAMING
# ============================================================
def run_stream():
    # Hứng tham số từ Terminal
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
        raise Exception(f"✗ Không tìm thấy Model tại: {args.model_path}")

    # Dùng đúng Kafka connector khớp với Spark runtime đang cài
    kafka_package = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1"
    print(f"Loading stable Kafka package: {kafka_package}")

    spark = (
        SparkSession.builder
        .appName("YouTubeTrendingRealtimePrediction")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.jars.packages", kafka_package) # Tải tự động package
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 80)
    print("YOUTUBE TRENDING REALTIME PREDICTION (KAFKA)")
    print("=" * 80)

    print(f"Loading trained model from {args.model_path}...")
    model = PipelineModel.load(args.model_path)
    print("✓ Model loaded successfully")

    print(f"Connecting to Kafka stream (Topic: {args.input_topic})...")
    stream_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", args.kafka_servers)
        .option("subscribe", args.input_topic)
        .option("startingOffsets", "latest")
        .load()
    )

    raw_schema = get_streaming_schema()
    parsed_struct_df = stream_df.select(
        F.from_json(F.col("value").cast("string"), raw_schema).alias("data")
    )

    # Drop malformed JSON fragments before downstream defaults mask data issues.
    parsed_df = (
        parsed_struct_df
        .filter(F.col("data").isNotNull())
        .select("data.*")
        .filter(F.col("video_id").isNotNull() & (F.length(F.trim(F.col("video_id"))) > 0))
    )

    # Đánh dấu thời gian lúc luồng dữ liệu chạy vào làm trending_date giả lập
    parsed_df = parsed_df.withColumn("parsed_trending_date", F.current_timestamp())

    print("Applying feature engineering...")
    features_df = apply_streaming_features(parsed_df)

    predictions = model.transform(features_df)

    # Đảo ngược Log về số ngày thực tế
    predictions = predictions.withColumn(
        "predicted_trending_days",
        F.round(F.expr("expm1(raw_prediction)"), 2)
    )

    output_df = predictions.select(
        F.col("video_id"),
        F.when(F.col("title").isNull(), F.lit("Unknown Title")).otherwise(F.col("title")).alias("title"),
        F.when(F.col("country").isNull(), F.lit("Unknown")).otherwise(F.col("country")).alias("country"),
        F.col("views_normalized").alias("views"),
        F.col("predicted_trending_days"),
        F.current_timestamp().alias("prediction_time")
    )

    # ĐÓNG GÓI JSON VÀ GỬI SANG TOPIC PREDICTIONS CHO CONSUMER ĐỌC
    print(f"Starting realtime prediction stream to Kafka ({args.output_topic})...")
    kafka_output_df = output_df.select(F.to_json(F.struct("*")).alias("value"))
    checkpoint_dir = resolve_checkpoint_dir(args.checkpoint_dir, args.checkpoint_policy)
    print(f"Using checkpoint location: {checkpoint_dir}")

    query = (
        kafka_output_df.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", args.kafka_servers)
        .option("topic", args.output_topic)
        .option("checkpointLocation", checkpoint_dir)
        .start()
    )

    print("✓ Streaming started successfully")
    print("=" * 80)
    query.awaitTermination()

if __name__ == "__main__":
    run_stream()