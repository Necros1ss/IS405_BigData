#!/usr/bin/env python3
"""Build a leak-resistant training dataset for trending-day prediction."""

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType

from app.utils.feature_engineering import add_shared_features


RENAME_MAP = {
    "video_published_at": "publish_date",
    "publishedAt": "publish_date",
    "video_trending__date": "trending_date",
    "video_trending_date": "trending_date",
    "video_view_count": "view_count",
    "views": "view_count",
    "video_like_count": "likes",
    "video_comment_count": "comment_count",
    "video_title": "title",
    "video_description": "description",
    "video_tags": "tags",
    "video_category_id": "categoryId",
    "video_trending_country": "country",
}

OPTIONAL_DEFAULTS = {
    "country": "UNKNOWN",
    "categoryId": "0",
    "title": "",
    "description": "",
    "tags": "",
}

REQUIRED_COLS = ["video_id", "publish_date", "trending_date", "view_count", "likes", "comment_count"]


def _safe_to_timestamp(column_name):
    return F.to_timestamp(
        F.when(F.length(F.trim(F.coalesce(F.col(column_name).cast("string"), F.lit("")))) == 0, F.lit(None)).otherwise(F.col(column_name))
    )


def standardize_columns(df):
    for old_col, new_col in RENAME_MAP.items():
        if old_col in df.columns and old_col != new_col:
            df = df.withColumnRenamed(old_col, new_col)

    for col_name, default_val in OPTIONAL_DEFAULTS.items():
        if col_name not in df.columns:
            df = df.withColumn(col_name, F.lit(default_val))

    missing_required = [column for column in REQUIRED_COLS if column not in df.columns]
    if missing_required:
        raise ValueError(f"Missing required columns: {missing_required}")

    for col_name in ["view_count", "likes", "comment_count"]:
        df = df.withColumn(col_name, F.regexp_replace(F.col(col_name).cast("string"), ",", "").cast(DoubleType()))

    publish_date_str = F.trim(F.coalesce(F.col("publish_date").cast("string"), F.lit("")))

    df = df.withColumn(
        "publish_timestamp",
        F.when(
            F.length(publish_date_str) == 0,
            F.lit(None).cast("timestamp"),
        ).otherwise(F.to_timestamp(publish_date_str, "yyyy-MM-dd'T'HH:mm:ssX")),
    )
    df = df.withColumn(
        "trending_date_parsed",
        F.coalesce(
            F.to_date("trending_date", "yyyy.MM.dd"),
            F.to_date("trending_date", "yyyy-MM-dd"),
            F.to_date("trending_date", "yy.dd.MM"),
            F.to_date("trending_date", "MM/dd/yyyy"),
        ),
    )

    df = df.filter(F.col("video_id").isNotNull())
    df = df.filter(F.col("publish_timestamp").isNotNull())
    df = df.filter(F.col("trending_date_parsed").isNotNull())
    df = df.dropDuplicates(["video_id", "country", "trending_date_parsed"])

    return df


def assign_trending_episodes(df, max_allowed_gap_days=1):
    window_sort = Window.partitionBy("video_id", "country").orderBy("trending_date_parsed")
    df = df.withColumn("prev_date", F.lag("trending_date_parsed").over(window_sort))
    df = df.withColumn("date_gap", F.datediff("trending_date_parsed", "prev_date"))
    df = df.withColumn(
        "episode_start_flag",
        F.when(F.col("date_gap").isNull() | (F.col("date_gap") > max_allowed_gap_days), 1).otherwise(0),
    )
    df = df.withColumn(
        "episode_id",
        F.sum("episode_start_flag").over(window_sort.rowsBetween(Window.unboundedPreceding, Window.currentRow)),
    )
    return df


def build_training_frame(df):
    df = standardize_columns(df)
    df = df.filter((F.col("view_count") >= 0) & (F.col("likes") >= 0) & (F.col("comment_count") >= 0))
    df = df.filter(F.col("likes") <= F.col("view_count"))
    df = assign_trending_episodes(df)

    episode_targets = (
        df.groupBy("video_id", "country", "episode_id")
        .agg(
            F.count("*").cast("double").alias("trending_days"),
            F.min("trending_date_parsed").alias("episode_start_date"),
            F.max("trending_date_parsed").alias("episode_end_date"),
        )
        .withColumn("log_trending_days", F.log1p(F.col("trending_days")))
    )

    first_snapshot = (
        df.withColumn("row_num", F.row_number().over(Window.partitionBy("video_id", "country", "episode_id").orderBy("trending_date_parsed")))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )

    final_df = first_snapshot.join(episode_targets, on=["video_id", "country", "episode_id"], how="inner")
    final_df = final_df.withColumn("reappear_count", (F.col("episode_id") - F.lit(1)).cast("double"))
    final_df = add_shared_features(final_df, snapshot_ts_col="trending_date_parsed")

    final_df = final_df.select(
        "video_id",
        "country",
        "categoryId",
        "episode_id",
        "trending_date_parsed",
        "episode_start_date",
        "episode_end_date",
        "trending_days",
        "log_trending_days",
        "reappear_count",
        "views_normalized",
        "log_views",
        "log_likes",
        "log_comment_count",
        "engagement_rate",
        "like_ratio",
        "comment_ratio",
        "views_per_hour",
        "likes_per_hour",
        "comments_per_hour",
        "title_length",
        "description_length",
        "tag_count",
        "publish_hour",
        "publish_day_of_week",
        "publish_dow",
        "log_hours_to_trending",
        "hours_to_trending",
        "title",
        "description",
        "tags",
        "publish_timestamp",
        "view_count",
        "likes",
        "comment_count",
    )

    return final_df.dropna(subset=["trending_days", "log_trending_days", "log_views", "log_likes", "log_comment_count"])
