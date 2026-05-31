#!/usr/bin/env python3
"""Shared feature engineering for batch training and streaming inference."""

from pyspark.sql import functions as F


def _ensure_string_column(df, column_name, default_value=""):
    if column_name not in df.columns:
        return df.withColumn(column_name, F.lit(default_value))
    return df.withColumn(
        column_name,
        F.coalesce(F.col(column_name).cast("string"), F.lit(default_value)),
    )


def _ensure_numeric_column(df, column_name, default_value=0.0):
    if column_name not in df.columns:
        return df.withColumn(column_name, F.lit(float(default_value)))
    return df.withColumn(
        column_name,
        F.regexp_replace(F.col(column_name).cast("string"), ",", "").cast("double"),
    )


def _safe_timestamp_expr(column_name):
    return F.when(F.length(F.trim(F.coalesce(F.col(column_name).cast("string"), F.lit("")))) == 0, F.lit(None)).otherwise(
        F.col(column_name)
    )


def add_shared_features(
    df,
    snapshot_ts_col,
    publish_ts_col="publish_timestamp",
    views_cols=("views", "view_count"),
    likes_col="likes",
    comments_col="comment_count",
    title_col="title",
    description_col="description",
    tags_col="tags",
):
    """Add a consistent set of numeric, ratio and text features."""

    for col_name in (publish_ts_col, title_col, description_col, tags_col):
        df = _ensure_string_column(df, col_name)

    for col_name in views_cols + (likes_col, comments_col):
        df = _ensure_numeric_column(df, col_name)

    df = df.withColumn(
        "views_normalized",
        F.coalesce(*[F.col(col_name) for col_name in views_cols], F.lit(0.0)),
    )

    if snapshot_ts_col in df.columns:
        df = df.withColumn("snapshot_timestamp", F.col(snapshot_ts_col))
    else:
        df = df.withColumn("snapshot_timestamp", F.current_timestamp())

    df = df.withColumn("publish_timestamp_parsed", F.to_timestamp(_safe_timestamp_expr(publish_ts_col)))

    df = df.withColumn(
        "hours_to_trending",
        (F.unix_timestamp(F.col("snapshot_timestamp")) - F.unix_timestamp(F.col("publish_timestamp_parsed"))) / 3600.0,
    )
    df = df.withColumn("hours_to_trending", F.when(F.col("hours_to_trending") < 0, 0.0).otherwise(F.col("hours_to_trending")))
    df = df.withColumn("log_hours_to_trending", F.log1p(F.coalesce(F.col("hours_to_trending"), F.lit(0.0))))

    df = df.withColumn("title_length", F.length(F.coalesce(F.col(title_col), F.lit(""))).cast("double"))
    df = df.withColumn("description_length", F.length(F.coalesce(F.col(description_col), F.lit(""))).cast("double"))
    df = df.withColumn(
        "tag_count",
        F.when(F.length(F.trim(F.coalesce(F.col(tags_col), F.lit("")))) == 0, F.lit(0.0)).otherwise(
            F.size(F.split(F.regexp_replace(F.col(tags_col), r"\s*\|\s*", "|"), r"\|"))
        ).cast("double"),
    )

    df = df.withColumn("publish_hour", F.coalesce(F.hour(F.col("publish_timestamp_parsed")).cast("double"), F.lit(12.0)))
    df = df.withColumn("publish_day_of_week", F.coalesce(F.dayofweek(F.col("publish_timestamp_parsed")).cast("double"), F.lit(1.0)))
    df = df.withColumn("publish_dow", F.col("publish_day_of_week"))

    df = df.withColumn("log_views", F.log1p(F.greatest(F.col("views_normalized"), F.lit(0.0))))
    df = df.withColumn("log_likes", F.log1p(F.greatest(F.col(likes_col), F.lit(0.0))))
    df = df.withColumn("log_comment_count", F.log1p(F.greatest(F.col(comments_col), F.lit(0.0))))

    df = df.withColumn(
        "like_ratio",
        F.when(F.col("views_normalized") > 0, F.col(likes_col) / F.col("views_normalized")).otherwise(0.0),
    )
    df = df.withColumn(
        "comment_ratio",
        F.when(F.col("views_normalized") > 0, F.col(comments_col) / F.col("views_normalized")).otherwise(0.0),
    )
    df = df.withColumn(
        "engagement_rate",
        F.when(
            F.col("views_normalized") > 0,
            (F.col(likes_col) + F.col(comments_col)) / F.col("views_normalized"),
        ).otherwise(0.0),
    )

    df = df.withColumn(
        "views_per_hour",
        F.when(F.col("hours_to_trending") > 0, F.col("views_normalized") / F.col("hours_to_trending")).otherwise(0.0),
    )
    df = df.withColumn(
        "likes_per_hour",
        F.when(F.col("hours_to_trending") > 0, F.col(likes_col) / F.col("hours_to_trending")).otherwise(0.0),
    )
    df = df.withColumn(
        "comments_per_hour",
        F.when(F.col("hours_to_trending") > 0, F.col(comments_col) / F.col("hours_to_trending")).otherwise(0.0),
    )

    return df
