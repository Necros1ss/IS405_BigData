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
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    # ============================================================
    # LOAD CSV
    # ============================================================

    logger.info("🚀 Loading dataset with Spark...")

    df = (
        spark.read
        .option("header", True)
        .option("multiLine", True)
        .option("quote", '"')
        .option("escape", '"')
        .option("mode", "PERMISSIVE")
        .option("inferSchema", False)
        .csv(RAW_DATA_PATH)
    )

    logger.info(f"✅ Loaded {df.count():,} raw rows")

    logger.info("📋 RAW COLUMNS:")
    logger.info(df.columns)

    # ============================================================
    # COLUMN STANDARDIZATION
    # ============================================================

    rename_map = {
        "video_published_at": "publish_date",
        "publishedAt": "publish_date",

        "video_trending__date": "trending_date",
        "video_trending_date": "trending_date",

        "video_view_count": "view_count",
        "views": "view_count",

        "video_like_count": "likes",

        "video_comment_count": "comment_count",

        "video_title": "title",

        "video_category_id": "categoryId",

        "video_trending_country": "country"
    }

    for old_col, new_col in rename_map.items():
        if old_col in df.columns and old_col != new_col:
            df = df.withColumnRenamed(old_col, new_col)

    logger.info("📋 COLUMNS AFTER RENAME:")
    logger.info(df.columns)

    # ============================================================
    # ADD OPTIONAL COLUMNS
    # ============================================================

    optional_defaults = {
        "country": "UNKNOWN",
        "categoryId": "0",
        "title": ""
    }

    for col_name, default_val in optional_defaults.items():

        if col_name not in df.columns:

            logger.warning(
                f"⚠️ Missing optional column: {col_name}"
            )

            df = df.withColumn(
                col_name,
                F.lit(default_val)
            )

    # ============================================================
    # REQUIRED COLUMNS CHECK
    # ============================================================

    required_cols = [
        "video_id",
        "publish_date",
        "trending_date",
        "view_count",
        "likes",
        "comment_count"
    ]

    missing_required = [
        c for c in required_cols
        if c not in df.columns
    ]

    if missing_required:

        raise Exception(
            f"\n❌ Missing required columns: {missing_required}\n"
            f"Available columns:\n{df.columns}"
        )

    # ============================================================
    # NUMERIC CLEANING
    # ============================================================

    logger.info("🧹 Cleaning numeric columns...")

    numeric_cols = [
        "view_count",
        "likes",
        "comment_count"
    ]

    for col_name in numeric_cols:

        df = df.withColumn(
            col_name,
            F.regexp_replace(
                F.col(col_name).cast("string"),
                ",",
                ""
            ).cast(DoubleType())
        )

    # ============================================================
    # DATE PARSING
    # ============================================================

    logger.info("📅 Parsing datetime columns...")

    df = df.withColumn(
        "publish_timestamp",
        F.coalesce(
            F.to_timestamp(
                "publish_date",
                "yyyy-MM-dd'T'HH:mm:ss'Z'"
            ),
            F.to_timestamp(
                "publish_date",
                "yyyy-MM-dd HH:mm:ss"
            ),
            F.to_timestamp("publish_date")
        )
    )

    df = df.withColumn(
        "trending_date_parsed",
        F.coalesce(
            F.to_date(
                "trending_date",
                "yyyy.MM.dd"
            ),
            F.to_date(
                "trending_date",
                "yyyy-MM-dd"
            ),
            F.to_date(
                "trending_date",
                "yy.dd.MM"
            ),
            F.to_date(
                "trending_date",
                "MM/dd/yyyy"
            )
        )
    )

    publish_valid = (
        df.filter(
            F.col("publish_timestamp").isNotNull()
        ).count()
    )

    trending_valid = (
        df.filter(
            F.col("trending_date_parsed").isNotNull()
        ).count()
    )

    logger.info(f"✅ publish_timestamp valid : {publish_valid:,}")
    logger.info(f"✅ trending_date valid     : {trending_valid:,}")

    # ============================================================
    # REMOVE INVALIDS & DUPLICATES
    # ============================================================

    logger.info("🧹 Removing invalid rows & duplicates...")

    df = (
        df
        .filter(F.col("video_id").isNotNull())
        .filter(F.col("trending_date_parsed").isNotNull())
        .dropDuplicates([
            "video_id",
            "country",
            "trending_date_parsed"
        ])
    )

    logger.info(f"✅ After dedupe: {df.count():,} rows")

    # ============================================================
    # GAPS & ISLANDS
    # ============================================================

    logger.info("📊 Computing streaks...")

    # 1. Window KHÔNG CÓ FRAME (Chỉ dùng cho lag/lead)
    window_sort = (
        Window
        .partitionBy("video_id", "country")
        .orderBy("trending_date_parsed")
    )

    df = df.withColumn(
        "prev_date",
        F.lag("trending_date_parsed").over(window_sort)
    )

    df = df.withColumn(
        "date_gap",
        F.datediff(
            "trending_date_parsed",
            "prev_date"
        )
    )

    df = df.withColumn(
        "is_new_streak",
        F.when(
            F.col("date_gap").isNull() | (F.col("date_gap") > 1),
            1
        ).otherwise(0)
    )

    # 2. Window CÓ FRAME (Dùng để cộng dồn/tích lũy - sum)
    window_cumsum = (
        Window
        .partitionBy("video_id", "country")
        .orderBy("trending_date_parsed")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    df = df.withColumn(
        "streak_id",
        F.sum("is_new_streak").over(window_cumsum)
    )

    streak_df = (
        df
        .groupBy("video_id", "country", "streak_id")
        .agg(
            F.count("*")
            .cast(DoubleType())
            .alias("streak_len")
        )
    )

    # ============================================================
    # TARGET ENGINEERING
    # ============================================================

    logger.info("🎯 Building target variables...")

    target_df = (
        streak_df
        .groupBy("video_id", "country")
        .agg(
            F.max("streak_len")
            .alias("trending_days"),

            (
                F.countDistinct("streak_id") - 1
            ).alias("reappear_count")
        )
    )

    target_df = (
        target_df
        .filter(F.col("trending_days") > 1)
    )

    target_df = target_df.withColumn(
        "log_trending_days",
        F.log1p(F.col("trending_days"))
    )

    logger.info(f"✅ Target rows: {target_df.count():,}")

    # ============================================================
    # DAY-1 SNAPSHOT
    # ============================================================

    logger.info("📸 Creating day-1 snapshot...")

    window_day1 = (
        Window
        .partitionBy("video_id", "country")
        .orderBy("trending_date_parsed")
    )

    day1_df = (
        df
        .withColumn(
            "row_num",
            F.row_number().over(window_day1)
        )
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )

    final_df = (
        day1_df
        .join(
            target_df,
            on=["video_id", "country"], 
            how="inner"
        )
    )

    logger.info(f"✅ After join: {final_df.count():,}")

    # ============================================================
    # INVALID DATA FILTERING
    # ============================================================

    logger.info("🧹 Filtering invalid values...")

    invalid_cond = (
        (F.col("view_count") < 0) |
        (F.col("likes") < 0) |
        (F.col("comment_count") < 0) |
        (F.col("likes") > F.col("view_count"))
    )

    final_df = final_df.filter(~invalid_cond)

    # ============================================================
    # FEATURE ENGINEERING
    # ============================================================

    logger.info("⚙️ Engineering features...")

    final_df = final_df.withColumn(
        "title_length",
        F.length(
            F.coalesce(
                F.col("title"),
                F.lit("")
            )
        ).cast(DoubleType())
    )

    final_df = final_df.withColumn(
        "publish_hour",
        F.coalesce(
            F.hour("publish_timestamp")
            .cast(DoubleType()),
            F.lit(12.0)
        )
    )

    final_df = final_df.withColumn(
        "publish_dow",
        F.coalesce(
            F.dayofweek("publish_timestamp")
            .cast(DoubleType()),
            F.lit(1.0)
        )
    )

    final_df = final_df.withColumn(
        "hours_to_trending",
        (
            F.unix_timestamp(
                "trending_date_parsed"
            ) -
            F.unix_timestamp(
                "publish_timestamp"
            )
        ) / 3600.0
    )

    final_df = final_df.withColumn(
        "hours_to_trending",
        F.when(
            F.col("hours_to_trending") < 0,
            0.0
        ).otherwise(
            F.col("hours_to_trending")
        )
    )

    final_df = final_df.withColumn(
        "log_hours_to_trending",
        F.log1p(
            F.coalesce(
                F.col("hours_to_trending"),
                F.lit(0.0)
            )
        )
    )

    final_df = final_df.withColumn(
        "log_views",
        F.log1p(
            F.greatest(
                F.coalesce(
                    F.col("view_count"),
                    F.lit(0.0)
                ),
                F.lit(0.0)
            )
        )
    )

    final_df = final_df.withColumn(
        "log_likes",
        F.log1p(
            F.greatest(
                F.coalesce(
                    F.col("likes"),
                    F.lit(0.0)
                ),
                F.lit(0.0)
            )
        )
    )

    final_df = final_df.withColumn(
        "log_comment_count",
        F.log1p(
            F.greatest(
                F.coalesce(
                    F.col("comment_count"),
                    F.lit(0.0)
                ),
                F.lit(0.0)
            )
        )
    )

    final_df = final_df.withColumn(
        "like_ratio",
        F.when(
            F.col("view_count") > 0,
            F.col("likes") /
            F.col("view_count")
        ).otherwise(0.0)
    )

    final_df = final_df.withColumn(
        "comment_ratio",
        F.when(
            F.col("view_count") > 0,
            F.col("comment_count") /
            F.col("view_count")
        ).otherwise(0.0)
    )

    # ============================================================
    # FINAL FEATURES
    # ============================================================

    logger.info("📦 Selecting final columns...")

    cols_to_keep = [
        "video_id",
        "trending_date_parsed",
        "trending_days",
        "log_trending_days",
        "reappear_count",
        "log_views",
        "log_likes",
        "log_comment_count",
        "like_ratio",
        "comment_ratio",
        "title_length",
        "publish_hour",
        "publish_dow",
        "log_hours_to_trending",
        "country",
        "categoryId"
    ]

    clean_df = final_df.select(*cols_to_keep)

    critical_cols = [
        "trending_days",
        "log_views",
        "log_likes",
        "log_comment_count"
    ]

    clean_df = clean_df.dropna(
        subset=critical_cols
    )

    # ============================================================
    # SAVE OUTPUT
    # ============================================================

    logger.info("💾 Caching final dataset...")

    clean_df.cache()

    final_count = clean_df.count()

    if final_count == 0:

        logger.warning(
            "⚠️ No data left after cleaning!"
        )

    else:

        logger.info(
            f"✅ Final cleaned rows: {final_count:,}"
        )

        os.makedirs(
            os.path.dirname(CLEAN_DATA_PATH),
            exist_ok=True
        )

        (
            clean_df
            .write
            .mode("overwrite")
            .parquet(CLEAN_DATA_PATH)
        )

        logger.info(
            "✅ Saved cleaned parquet successfully!"
        )

    clean_df.unpersist()

    spark.stop()

# ============================================================
# ENTRYPOINT
# ============================================================

if __name__ == "__main__":
    clean_data()