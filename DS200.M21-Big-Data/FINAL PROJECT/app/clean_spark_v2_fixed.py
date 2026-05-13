#!/usr/bin/env python3

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType

def clean_data():
    spark = (
        SparkSession.builder
        .appName("YouTubeTrendingRegressionCleaning")
        .config("spark.sql.shuffle.partitions", "20") # Nên để 20-50 thay vì 8 cho Big Data
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    INPUT_PATH = "data/GLOBAL_youtube_trending.csv"
    OUTPUT_PATH = "data/cleaned_youtube_regression.parquet"

    print("Loading dataset...")
    df = (
        spark.read
        .option("header", True)
        .option("multiLine", True)
        .option("escape", '"')
        .csv(INPUT_PATH)
    )

    print(f"Loaded {df.count():,} raw rows")

    # ============================================================
    # 1. COLUMN STANDARDIZATION & TYPE CONVERSION
    # ============================================================
    rename_map = {
        "video_published_at": "publish_date",
        "video_trending__date": "trending_date", # Có 2 dấu gạch dưới theo đúng schema của bạn
        "video_trending_date": "trending_date",  # Dự phòng 1 dấu gạch dưới
        "video_view_count": "view_count",
        "views": "view_count",                   # Dự phòng
        "video_like_count": "likes",
        "video_comment_count": "comment_count",
        "video_title": "title",
        "video_tags": "tags",
        "video_trending_country": "country"
    }

    for old_col, new_col in rename_map.items():
        if old_col in df.columns:
            df = df.withColumnRenamed(old_col, new_col)

    numeric_cols = ["view_count", "likes", "comment_count"]
    for col_name in numeric_cols:
        if col_name in df.columns:
            # Xóa dấu phẩy và ép kiểu Double
            df = df.withColumn(col_name, F.regexp_replace(F.col(col_name), ",", "").cast(DoubleType()))

    # Parse dates to timestamp 
    df = df.withColumn("publish_timestamp", F.to_timestamp("publish_date"))
    df = df.withColumn(
        "trending_date_parsed",
        F.coalesce(
            F.to_date("trending_date", "yyyy.MM.dd"), # Định dạng trong file youtube_2024 của bạn
            F.to_date("trending_date", "yyyy-MM-dd"), # Chuẩn ISO
            F.to_date("trending_date", "yy.dd.MM")    # Định dạng Kaggle bản cũ
        )
    )

    # ============================================================
    # LỌC DỮ LIỆU TỪ 04/2025 ĐẾN 04/2026 (Theo video_published_at)
    # ============================================================
    print("Filtering data from April 2025 to April 2026...")
    df = df.filter(
        (F.col("publish_timestamp") >= "2025-04-01 00:00:00") & 
        (F.col("publish_timestamp") <= "2026-04-30 23:59:59")
    )
    print(f"Rows remaining after filter: {df.count():,}")

    # ============================================================
    # 2. TARGET ENGINEERING: GAPS AND ISLANDS (Max Consecutive Streak)
    # ============================================================
    print("Calculating Target (trending_days)...")
    window_streak = Window.partitionBy("video_id").orderBy("trending_date_parsed")
    df_streak = df.withColumn("rn", F.row_number().over(window_streak))
    
    # Trừ ngày trending cho số thứ tự để tìm các chuỗi liên tiếp
    df_streak = df_streak.withColumn("anchor_date", F.expr("date_sub(trending_date_parsed, rn)"))
    streak_counts = df_streak.groupBy("video_id", "anchor_date").agg(F.count("*").cast(DoubleType()).alias("streak_len"))
    
    target_df = streak_counts.groupBy("video_id").agg(F.max("streak_len").alias("trending_days"))
    target_df = target_df.filter(F.col("trending_days") > 1) # Chỉ giữ video trend từ 2 ngày trở lên
    target_df = target_df.withColumn("log_trending_days", F.log1p(F.col("trending_days")))

    # ============================================================
    # 3. ANTI-LEAKAGE: DAY-1 SNAPSHOT
    # ============================================================
    print("Extracting Day-1 Snapshot...")
    window_day1 = Window.partitionBy("video_id").orderBy("trending_date_parsed")
    day1_df = df.withColumn("row_num", F.row_number().over(window_day1)) \
                .filter(F.col("row_num") == 1).drop("row_num")

    final_df = day1_df.join(target_df, on="video_id", how="inner")

    # ============================================================
    # 4. FEATURE ENGINEERING
    # ============================================================
    print("Applying Feature Engineering...")
    final_df = final_df \
        .withColumn("title_length", F.when(F.col("title").isNull(), 0.0).otherwise(F.length(F.col("title")).cast(DoubleType()))) \
        .withColumn("publish_hour", F.when(F.col("publish_timestamp").isNull(), 12.0).otherwise(F.hour(F.col("publish_timestamp")).cast(DoubleType()))) \
        .withColumn("publish_dow", F.when(F.col("publish_timestamp").isNull(), 1.0).otherwise(F.dayofweek(F.col("publish_timestamp")).cast(DoubleType())))

    # Khoảng cách từ lúc Publish tới lúc Trending (Tính bằng giờ)
    final_df = final_df.withColumn(
        "hours_to_trending",
        (F.unix_timestamp(F.col("trending_date_parsed")) - F.unix_timestamp(F.col("publish_timestamp"))) / 3600.0
    )
    final_df = final_df.withColumn("hours_to_trending", F.when(F.col("hours_to_trending") < 0, 0.0).otherwise(F.col("hours_to_trending")))
    final_df = final_df.withColumn("log_hours_to_trending", F.log1p(F.col("hours_to_trending")))

    # Log-transform biến đếm & Ratios
    final_df = final_df \
        .withColumn("log_views", F.log1p(F.col("view_count"))) \
        .withColumn("log_likes", F.log1p(F.col("likes"))) \
        .withColumn("log_comment_count", F.log1p(F.col("comment_count"))) \
        .withColumn("like_ratio", F.when(F.col("view_count") == 0, 0.0).otherwise(F.col("likes") / F.col("view_count"))) \
        .withColumn("comment_ratio", F.when(F.col("view_count") == 0, 0.0).otherwise(F.col("comment_count") / F.col("view_count")))

    # ============================================================
    # 5. WRITE OUTPUT
    # ============================================================
    cols_to_keep = [
        "video_id", "trending_date_parsed", "trending_days", "log_trending_days",
        "log_views", "log_likes", "log_comment_count", "like_ratio", "comment_ratio",
        "title_length", "publish_hour", "publish_dow", "log_hours_to_trending"
    ]
    
    clean_df = final_df.select([F.col(c) for c in cols_to_keep]).dropna()
    
    # Kiểm tra an toàn trước khi lưu
    if clean_df.count() == 0:
        print("CẢNH BÁO: Không có dữ liệu nào còn lại sau khi clean. Hãy kiểm tra định dạng ngày tháng hoặc mốc thời gian filter!")
    else:
        print(f"Writing {clean_df.count():,} cleaned rows to Parquet...")
        os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
        clean_df.write.mode("overwrite").parquet(OUTPUT_PATH)
        print("✓ Xong!")

    spark.stop()

if __name__ == "__main__":
    clean_data()