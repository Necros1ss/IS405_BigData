import logging
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.window import Window

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ==============================================================================
# 1. SHARED MODULES & EXPLICIT SCHEMA
# ==============================================================================
def get_shared_schema():
    """Explicit Schema chuẩn Production (Không dùng inferSchema)"""
    return StructType([
        StructField("video_id", StringType(), True),
        StructField("trending_date", StringType(), True), # Raw date từ Kaggle
        StructField("title", StringType(), True),
        StructField("channel_title", StringType(), True),
        StructField("category_id", StringType(), True),
        StructField("publish_time", StringType(), True),  # Raw publish time
        StructField("tags", StringType(), True),
        StructField("views", DoubleType(), True),
        StructField("likes", DoubleType(), True),
        StructField("dislikes", DoubleType(), True),
        StructField("comment_count", DoubleType(), True),
        StructField("thumbnail_link", StringType(), True),
        StructField("comments_disabled", StringType(), True),
        StructField("ratings_disabled", StringType(), True),
        StructField("video_error_or_removed", StringType(), True),
        StructField("description", StringType(), True),
        # Feature phụ trợ thêm vào khi streaming (Producer cần gửi lên)
        StructField("country", StringType(), True) 
    ])

def apply_shared_feature_engineering(df):
    """Module Feature Engineering duy nhất"""
    df = df.withColumn("title_length", F.when(F.col("title").isNull(), F.lit(0.0)).otherwise(F.length(F.col("title")).cast(DoubleType()))) \
           .withColumn("tag_count", F.when(F.col("tags").isNull(), F.lit(0.0)).otherwise(F.size(F.split(F.col("tags"), "\\|")).cast(DoubleType())))
    
    df = df.withColumn("publish_date_parsed", F.expr("TRY_CAST(publish_time AS timestamp)")) \
           .withColumn("publish_hour", F.when(F.col("publish_date_parsed").isNull(), F.lit(12.0)).otherwise(F.hour(F.col("publish_date_parsed")).cast(DoubleType()))) \
           .withColumn("publish_dow", F.when(F.col("publish_date_parsed").isNull(), F.lit(1.0)).otherwise(F.dayofweek(F.col("publish_date_parsed")).cast(DoubleType())))

    # VIDEO AGE (Hours to Trending)
    df = df.withColumn("hours_to_trending", (F.unix_timestamp(F.col("parsed_trending_date")) - F.unix_timestamp(F.col("publish_date_parsed"))) / 3600.0)
    df = df.withColumn("hours_to_trending", F.when(F.col("hours_to_trending") < 0, 0.0).otherwise(F.col("hours_to_trending")))
    df = df.withColumn("log_hours_to_trending", F.log1p(F.col("hours_to_trending")))

    # LOG TRANSFORM & RATIOS
    df = df.withColumn("log_views", F.log1p(F.col("views"))) \
           .withColumn("log_likes", F.log1p(F.col("likes"))) \
           .withColumn("log_comment_count", F.log1p(F.col("comment_count"))) \
           .withColumn("like_ratio", F.when(F.col("views") == 0, 0.0).otherwise(F.col("likes") / F.col("views"))) \
           .withColumn("comment_ratio", F.when(F.col("views") == 0, 0.0).otherwise(F.col("comment_count") / F.col("views")))
           
    return df

# ==============================================================================
# 2. BATCH ETL PIPELINE
# ==============================================================================
def clean_data():
    logger.info("Khởi tạo Spark Session cho ETL Pipeline...")
    spark = SparkSession.builder \
        .appName("YouTubeTrending_ETL_Optimized") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "20") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    # CHÚ Ý: Load multi-country dataset bằng wildcard *
    data_path = os.path.join(project_root, "data", "*videos.csv")
    
    logger.info(f"Đang đọc dữ liệu từ: {data_path} (Với Explicit Schema)")
    raw_schema = get_shared_schema()
    
    # Bỏ inferSchema, sử dụng schema tĩnh
    df = spark.read.option("header", "true") \
        .option("multiLine", "true").option("escape", '"') \
        .schema(raw_schema).csv(data_path)

    # TRÍCH XUẤT COUNTRY TỪ TÊN FILE (Ví dụ: hdfs://.../USvideos.csv -> US)
    df = df.withColumn("file_name", F.input_file_name()) \
           .withColumn("country", F.regexp_extract(F.col("file_name"), r"([A-Z]{2})videos\.csv", 1)) \
           .drop("file_name")

    df = df.withColumn("parsed_trending_date", F.to_date(F.col("trending_date"), "yy.dd.MM"))
    df = df.fillna({"country": "UNKNOWN"}) # Đảm bảo không dính null

    # BƯỚC 1: TARGET GAPS AND ISLANDS
    logger.info("Đang tính toán Target (Max Consecutive Streak)...")
    window_streak = Window.partitionBy("video_id").orderBy("parsed_trending_date")
    df_streak = df.withColumn("rn", F.row_number().over(window_streak))
    df_streak = df_streak.withColumn("anchor_date", F.expr("date_sub(parsed_trending_date, rn)"))
    streak_counts = df_streak.groupBy("video_id", "anchor_date").agg(F.count("*").cast(DoubleType()).alias("streak_len"))
    
    target_df = streak_counts.groupBy("video_id").agg(F.max("streak_len").alias("trending_days"))
    target_df = target_df.filter(F.col("trending_days") > 1)
    target_df = target_df.withColumn("log_trending_days", F.log1p(F.col("trending_days")))

    # BƯỚC 2: DAY-1 SNAPSHOT
    logger.info("Đang trích xuất Day-1 Snapshot để chống Data Leakage...")
    windowSpec = Window.partitionBy("video_id").orderBy("parsed_trending_date")
    day1_df = df.withColumn("row_num", F.row_number().over(windowSpec)) \
                .filter(F.col("row_num") == 1).drop("row_num")

    final_df = day1_df.join(target_df, on="video_id", how="inner")
    
    # BƯỚC 3: FEATURE ENGINEERING
    final_df = apply_shared_feature_engineering(final_df)

    # BƯỚC 4: ANTI-LEAKAGE
    logger.info("Thực hiện kiểm tra Strict Anti-Leakage...")
    invalid_time_rows = final_df.filter(F.col("hours_to_trending") < 0).count()
    duplicate_videos = final_df.groupBy("video_id").count().filter(F.col("count") > 1).count()
    if invalid_time_rows > 0 or duplicate_videos > 0:
        raise ValueError("LEAKAGE ALERT: Data bị lỗi logic timeline hoặc duplicate!")

    cols_to_keep = [
        "video_id", "parsed_trending_date", "trending_days", "log_trending_days",
        "country", # CHÚ Ý: Bổ sung Country
        "log_views", "log_likes", "log_comment_count", "like_ratio", "comment_ratio",
        "title_length", "tag_count", "publish_hour", "publish_dow",
        "log_hours_to_trending"
    ]
    clean_df = final_df.select([F.col(c) for c in cols_to_keep]).dropna()
    
    output_path = os.path.join(project_root, "data", "cleaned_youtube_regression.parquet")
    logger.info(f"Đang ghi Parquet partition theo: country, publish_dow...")
    # Tối ưu: Phân vùng theo quốc gia trước, sau đó là ngày trong tuần
    clean_df.write.mode("overwrite").partitionBy("country", "publish_dow").parquet(output_path)
    logger.info(f"✓ Hoàn thành ETL. Dữ liệu lưu tại: {output_path}")
    spark.stop()

if __name__ == "__main__":
    clean_data()