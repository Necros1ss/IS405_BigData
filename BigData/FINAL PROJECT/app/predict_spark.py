"""
Prediction utilities for Spark ML pipeline.
"""
from app.spark_bootstrap import ensure_spark_runtime

ensure_spark_runtime()

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def predict_sample(model):
    """Generate and show sample predictions using the trained regression PipelineModel."""
    from pyspark.sql import Row

    samples = [
        Row(
            log_views=13.2,
            log_likes=10.8,
            log_comment_count=8.9,
            like_ratio=0.08,
            comment_ratio=0.015,
            title_length=64.0,
            publish_hour=20.0,
            publish_dow=6.0,
            log_hours_to_trending=3.0,
        ),
        Row(
            log_views=9.0,
            log_likes=6.5,
            log_comment_count=5.1,
            like_ratio=0.02,
            comment_ratio=0.003,
            title_length=38.0,
            publish_hour=11.0,
            publish_dow=3.0,
            log_hours_to_trending=2.1,
        ),
        Row(
            log_views=11.0,
            log_likes=8.6,
            log_comment_count=7.2,
            like_ratio=0.05,
            comment_ratio=0.01,
            title_length=52.0,
            publish_hour=18.0,
            publish_dow=7.0,
            log_hours_to_trending=2.8,
        ),
    ]
    spark_sess = SparkSession.builder.getOrCreate()
    df_new = spark_sess.createDataFrame(samples)
    preds = model.transform(df_new)
    preds = preds.withColumn(
        "predicted_trending_days",
        F.round(F.expr("expm1(raw_prediction)"), 2),
    )
    preds.select(
        "log_views",
        "log_likes",
        "log_comment_count",
        "like_ratio",
        "comment_ratio",
        "predicted_trending_days",
    ).show(truncate=False)
