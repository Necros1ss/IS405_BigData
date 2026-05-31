"""
Prediction utilities for Spark ML pipeline.
"""
import argparse
import os

from app.spark_bootstrap import ensure_spark_runtime

ensure_spark_runtime()

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def predict_from_dataset(model, data_path="data/cleaned_youtube_regression.parquet", sample_size=20, seed=42):
    """Run predictions on real rows loaded from the cleaned training dataset."""

    if not os.path.exists(data_path):
        raise FileNotFoundError(f"Dataset not found: {data_path}")

    spark_sess = SparkSession.builder.getOrCreate()
    df_new = spark_sess.read.parquet(data_path).orderBy(F.rand(seed)).limit(sample_size)
    preds = model.transform(df_new)
    preds = preds.withColumn(
        "predicted_trending_days",
        F.round(F.expr("expm1(raw_prediction)"), 2),
    )
    preds.select(
        "video_id",
        "country",
        "trending_date_parsed",
        "views_normalized",
        "log_views",
        "log_likes",
        "log_comment_count",
        "engagement_rate",
        "like_ratio",
        "comment_ratio",
        "predicted_trending_days",
    ).show(truncate=False)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--model-path", required=True)
    parser.add_argument("--data", default="data/cleaned_youtube_regression.parquet")
    parser.add_argument("--sample-size", type=int, default=20)
    args = parser.parse_args()

    from pyspark.ml import PipelineModel

    model = PipelineModel.load(args.model_path)
    predict_from_dataset(model, data_path=args.data, sample_size=args.sample_size)


if __name__ == "__main__":
    main()
