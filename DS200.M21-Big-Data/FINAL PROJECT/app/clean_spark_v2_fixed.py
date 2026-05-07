import logging
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.window import Window

# CẤU HÌNH LOGGING CHUẨN PRODUCTION
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ==============================================================================
# 1. SHARED MODULES (DÙNG CHUNG CHO CẢ TRAIN VÀ STREAMING)
# ==============================================================================
def get_shared_schema():
    """Lược đồ chuẩn xác để validate data từ Kafka và CSV"""
    return StructType([
        StructField("video_id", StringType(), True),
        StructField("title", StringType(), True),
        StructField("publish_date", StringType(), True),
        StructField("video_tags", StringType(), True),
        StructField("views", DoubleType(), True),
        StructField("likes", DoubleType(), True),
        StructField("comment_count", DoubleType(), True)
    ])

def apply_shared_feature_engineering(df):
    """Module Feature Engineering duy nhất (Chống Training-Serving Skew)"""
    # 1. Xử lý Metadata
    df = df.withColumn("title_length", F.when(F.col("title").isNull(), F.lit(0.0)).otherwise(F.length(F.col("title")).cast(DoubleType()))) \
           .withColumn("tag_count", F.when(F.col("video_tags").isNull(), F.lit(0.0)).otherwise(F.size(F.split(F.col("video_tags"), "\\|")).cast(DoubleType())))
    
    # 2. Xử lý Thời gian
    df = df.withColumn("publish_date_parsed", F.expr("TRY_CAST(publish_date AS timestamp)")) \
           .withColumn("publish_hour", F.when(F.col("publish_date_parsed").isNull(), F.lit(12.0)).otherwise(F.hour(F.col("publish_date_parsed")).cast(DoubleType()))) \
           .withColumn("publish_dow", F.when(F.col("publish_date_parsed").isNull(), F.lit(1.0)).otherwise(F.dayofweek(F.col("publish_date_parsed")).cast(DoubleType())))

    # 3. Xử lý Log-Transform & Ratios (Giảm Skewness)
    df = df.withColumn("log_views", F.log1p(F.col("views"))) \
           .withColumn("log_likes", F.log1p(F.col("likes"))) \
           .withColumn("log_comment_count", F.log1p(F.col("comment_count"))) \
           .withColumn("like_ratio", F.when(F.col("views") == 0, 0.0).otherwise(F.col("likes") / F.col("views"))) \
           .withColumn("comment_ratio", F.when(F.col("views") == 0, 0.0).otherwise(F.col("comment_count") / F.col("views")))
           
    return df

# ==============================================================================
# 2. BATCH ETL PIPELINE (Làm sạch dữ liệu Kaggle)
# ==============================================================================
def clean_data():
    logger.info("Khởi tạo Spark Session cho ETL Pipeline...")
    # Tối ưu hóa Spark (Memory & Partitions)
    spark = SparkSession.builder \
        .appName("YouTubeTrending_ETL_Optimized") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "20") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("ERROR")

    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_path = os.path.join(project_root, "data", "USvideos.csv")
    
    logger.info(f"Đang đọc dữ liệu từ: {data_path}")
    df = spark.read.option("header", "true").option("inferSchema", "true") \
        .option("multiLine", "true").option("escape", '"').csv(data_path)

    # Tiền xử lý các cột thô
    df = df.withColumn("parsed_trending_date", F.to_date(F.col("trending_date"), "yy.dd.MM")) \
           .withColumn("views", F.col("views").cast(DoubleType())) \
           .withColumn("likes", F.col("likes").cast(DoubleType())) \
           .withColumn("comment_count", F.col("comment_count").cast(DoubleType())) \
           .withColumn("publish_date", F.col("publish_time")) # Map sang tên cột chung

    # Tính Target (countDistinct)
    logger.info("Đang tính toán Target (trending_days) bằng countDistinct...")
    target_df = df.groupBy("video_id").agg(
        F.countDistinct("parsed_trending_date").cast(DoubleType()).alias("trending_days")
    )
    target_df = target_df.filter(F.col("trending_days") > 1)
    target_df = target_df.withColumn("log_trending_days", F.log1p(F.col("trending_days")))

    # Lấy Day-1 Snapshot (Chống Leakage)
    logger.info("Đang trích xuất Day-1 Snapshot để chống Data Leakage...")
    windowSpec = Window.partitionBy("video_id").orderBy("parsed_trending_date")
    day1_df = df.withColumn("row_num", F.row_number().over(windowSpec)) \
                .filter(F.col("row_num") == 1).drop("row_num")

    # Join target và Áp dụng SHARED FEATURE ENGINEERING
    final_df = day1_df.join(target_df, on="video_id", how="inner")
    final_df = apply_shared_feature_engineering(final_df)

    cols_to_keep = [
        "video_id", "parsed_trending_date", "trending_days", "log_trending_days",
        "log_views", "log_likes", "log_comment_count", "like_ratio", "comment_ratio",
        "title_length", "tag_count", "publish_hour", "publish_dow"
    ]
    clean_df = final_df.select([F.col(c) for c in cols_to_keep]).dropna()
    
    # GHI FILE TỐI ƯU (PARTITIONING)
    output_path = os.path.join(project_root, "data", "cleaned_youtube_regression.parquet")
    logger.info(f"Đang ghi file Parquet có phân vùng (Partition By Day of Week)...")
    clean_df.write.mode("overwrite").partitionBy("publish_dow").parquet(output_path)
    
    logger.info(f"✓ Hoàn thành ETL. Dữ liệu sạch lưu tại: {output_path}")
    spark.stop()

if __name__ == "__main__":
    clean_data()