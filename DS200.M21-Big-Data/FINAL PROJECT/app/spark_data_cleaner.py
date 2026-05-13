#!/usr/bin/env python3

import os
import logging

from app.spark_bootstrap import ensure_spark_runtime

ensure_spark_runtime()

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType

# ============================================================
# LOAD CONFIG
# ============================================================

try:
    from app.config import RAW_DATA_PATH, CLEAN_DATA_PATH, SPARK_APP_NAME
except ImportError:
    from config import RAW_DATA_PATH, CLEAN_DATA_PATH, SPARK_APP_NAME

# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

logger = logging.getLogger(__name__)

# ============================================================
# MAIN CLEANING FUNCTION
# ============================================================

def clean_data():

    # ============================================================
    # SPARK SESSION
    # ============================================================

    spark = (
        SparkSession.builder
        .appName(SPARK_APP_NAME)
        .config("spark.sql.shuffle.partitions", "32")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    logger.info("🚀 Loading dataset with Spark...")

    df = (
        spark.read
        .option("header", True)
        .option("multiLine", True)
        .option("escape", '"')
        .csv(RAW_DATA_PATH)
    )

    logger.info(f"Loaded {df.count():,} raw rows")

    # ============================================================
    # COLUMN STANDARDIZATION
    # ============================================================

    rename_map = {
        "video_published_at": "publish_date",
        "video_trending__date": "trending_date",
        "video_trending_date": "trending_date",
        "video_view_count": "view_count",
        "views": "view_count",
        "video_like_count": "likes",
        "video_comment_count": "comment_count",
        "video_title": "title"
    }

    for old_col, new_col in rename_map.items():
        if old_col in df.columns:
            df = df.withColumnRenamed(old_col, new_col)

    # ============================================================
    # NUMERIC CLEANING
    # ============================================================

    numeric_cols = ["view_count", "likes", "comment_count"]

    for col_name in numeric_cols:
        if col_name in df.columns:
            df = df.withColumn(
                col_name,
                F.regexp_replace(F.col(col_name), ",", "").cast(DoubleType())
            )

    # ============================================================
    # DATE PARSING
    # ============================================================

    logger.info("📅 Parsing datetime columns...")

    df = df.withColumn(
        "publish_timestamp",
        F.to_timestamp("publish_date")
    )

    df = df.withColumn(
        "trending_date_parsed",
        F.coalesce(
            F.to_date("trending_date", "yyyy.MM.dd"),  # 2024.03.15
            F.to_date("trending_date", "yyyy-MM-dd"),  # 2024-03-15
            F.to_date("trending_date", "yy.dd.MM")     # 24.15.03
        )
    )

    # ============================================================
    # REMOVE INVALIDS & DUPLICATES
    # ============================================================

    df = (
        df
        .filter(F.col("video_id").isNotNull())
        .filter(F.col("trending_date_parsed").isNotNull())
        .dropDuplicates(["video_id", "trending_date_parsed"])
    )

    # ============================================================
    # GAPS & ISLANDS
    # ============================================================

    window_sort = Window.partitionBy("video_id").orderBy("trending_date_parsed")

    df = df.withColumn("prev_date", F.lag("trending_date_parsed").over(window_sort))
    
    df = df.withColumn("date_gap", F.datediff("trending_date_parsed", "prev_date"))

    df = df.withColumn(
        "is_new_streak",
        F.when(
            F.col("date_gap").isNull() | (F.col("date_gap") > 1), 
            1
        ).otherwise(0)
    )

    window_cumsum = (
        Window
        .partitionBy("video_id")
        .orderBy("trending_date_parsed")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    df = df.withColumn("streak_id", F.sum("is_new_streak").over(window_cumsum))

    streak_df = (
        df
        .groupBy("video_id", "streak_id")
        .agg(F.count("*").cast(DoubleType()).alias("streak_len"))
    )

    # ============================================================
    # TARGET ENGINEERING
    # ============================================================

    target_df = (
        streak_df
        .groupBy("video_id")
        .agg(
            F.max("streak_len").alias("trending_days"),
            (F.countDistinct("streak_id") - 1).alias("reappear_count")
        )
    )

    target_df = target_df.filter(F.col("trending_days") > 1)

    target_df = target_df.withColumn(
        "log_trending_days",
        F.log1p(F.col("trending_days"))
    )

    # ============================================================
    # DAY-1 SNAPSHOT (ANTI-LEAKAGE)
    # ============================================================

    window_day1 = Window.partitionBy("video_id").orderBy("trending_date_parsed")

    day1_df = (
        df
        .withColumn("row_num", F.row_number().over(window_day1))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )

    final_df = day1_df.join(target_df, on="video_id", how="inner")

    # ============================================================
    # DATA VALIDATION & FILTERING (Vứt rác)
    # ============================================================

    invalid_cond = (
        (F.col("view_count") < 0) |
        (F.col("likes") < 0) |
        (F.col("comment_count") < 0) |
        (F.col("likes") > F.col("view_count"))
    )

    # Lọc bỏ luôn dữ liệu rác thay vì chỉ log
    final_df = final_df.filter(~invalid_cond)

    # ============================================================
    # FEATURE ENGINEERING
    # ============================================================

    final_df = final_df.withColumn(
        "title_length",
        F.length(F.coalesce(F.col("title"), F.lit(""))).cast(DoubleType())
    )

    final_df = final_df.withColumn(
        "publish_hour",
        F.coalesce(F.hour("publish_timestamp").cast(DoubleType()), F.lit(12.0))
    )

    final_df = final_df.withColumn(
        "publish_dow",
        F.coalesce(F.dayofweek("publish_timestamp").cast(DoubleType()), F.lit(1.0))
    )

    final_df = final_df.withColumn(
        "hours_to_trending",
        (F.unix_timestamp("trending_date_parsed") - F.unix_timestamp("publish_timestamp")) / 3600.0
    )

    final_df = final_df.withColumn(
        "hours_to_trending",
        F.when(F.col("hours_to_trending") < 0, 0.0).otherwise(F.col("hours_to_trending"))
    )

    final_df = final_df.withColumn(
        "log_hours_to_trending",
        F.log1p(F.col("hours_to_trending"))
    )

    final_df = final_df.withColumn(
        "log_views",
        F.log1p(F.greatest(F.col("view_count"), F.lit(0.0)))
    )

    final_df = final_df.withColumn(
        "log_likes",
        F.log1p(F.greatest(F.col("likes"), F.lit(0.0)))
    )

    final_df = final_df.withColumn(
        "log_comment_count",
        F.log1p(F.greatest(F.col("comment_count"), F.lit(0.0)))
    )

    final_df = final_df.withColumn(
        "like_ratio",
        F.when(F.col("view_count") > 0, F.col("likes") / F.col("view_count")).otherwise(0.0)
    )

    final_df = final_df.withColumn(
        "comment_ratio",
        F.when(F.col("view_count") > 0, F.col("comment_count") / F.col("view_count")).otherwise(0.0)
    )

    # ============================================================
    # FINAL FEATURES
    # ============================================================

    cols_to_keep = [
        "video_id", "trending_date_parsed", "trending_days", "log_trending_days",
        "reappear_count", "log_views", "log_likes", "log_comment_count",
        "like_ratio", "comment_ratio", "title_length", "publish_hour", "publish_dow",
        "log_hours_to_trending"
    ]

    clean_df = final_df.select(*cols_to_keep).dropna()

    # ============================================================
    # OPTIMIZATION: CACHE VÀ XUẤT FILE
    # ============================================================
    
    clean_df.cache() # Lưu RAM tạm để đếm nhanh, chống Lazy Evaluation

    logger.info("💾 Đang phân tích và đếm dữ liệu cuối...")
    final_count = clean_df.count()

    if final_count == 0:
        logger.warning("⚠️ Không có dữ liệu nào còn lại sau khi clean!")
    else:
        logger.info(f"✅ Data passed assertions. Ready to save {final_count:,} rows.")

        os.makedirs(os.path.dirname(CLEAN_DATA_PATH), exist_ok=True)

        (
            clean_df
            .write
            .mode("overwrite")
            .parquet(CLEAN_DATA_PATH)
        )

        logger.info("✅ Saved cleaned parquet successfully!")

    clean_df.unpersist() # Trả RAM cho hệ điều hành
    spark.stop()

# ============================================================
# ENTRYPOINT
# ============================================================

if __name__ == "__main__":
    clean_data()