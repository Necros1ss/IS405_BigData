from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType
from pyspark.sql.window import Window
import os

def clean_data():
    spark = SparkSession.builder \
        .appName("YouTubeTrending_ETL") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("ERROR")

    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_path = os.path.join(project_root, "data", "USvideos.csv") # Cập nhật đường dẫn data của bạn
    
    df = spark.read.option("header", "true").option("inferSchema", "true") \
        .option("multiLine", "true").option("escape", '"').csv(data_path)

    df = df.withColumn("parsed_trending_date", F.to_date(F.col("trending_date"), "yy.dd.MM")) \
           .withColumn("views", F.col("views").cast(DoubleType())) \
           .withColumn("likes", F.col("likes").cast(DoubleType())) \
           .withColumn("comment_count", F.col("comment_count").cast(DoubleType()))

    # 1. Tính Target (countDistinct) & Log-transform Target
    target_df = df.groupBy("video_id").agg(
        F.countDistinct("parsed_trending_date").cast(DoubleType()).alias("trending_days")
    )
    target_df = target_df.filter(F.col("trending_days") > 1)
    target_df = target_df.withColumn("log_trending_days", F.log1p(F.col("trending_days")))

    # 2. Ngăn Leakage: Chỉ lấy Day-1 Snapshot
    windowSpec = Window.partitionBy("video_id").orderBy("parsed_trending_date")
    day1_df = df.withColumn("row_num", F.row_number().over(windowSpec)) \
                .filter(F.col("row_num") == 1).drop("row_num")

    final_df = day1_df.join(target_df, on="video_id", how="inner")

    # 3. Feature Engineering (Log transform Skewed Data + Ratios)
    final_df = final_df \
        .withColumn("title_length", F.length(F.col("title")).cast(DoubleType())) \
        .withColumn("tag_count", F.size(F.split(F.col("tags"), "\|")).cast(DoubleType())) \
        .withColumn("publish_time", F.col("publish_time").cast("timestamp")) \
        .withColumn("publish_hour", F.hour(F.col("publish_time")).cast(DoubleType())) \
        .withColumn("publish_dow", F.dayofweek(F.col("publish_time")).cast(DoubleType())) \
        .withColumn("log_views", F.log1p(F.col("views"))) \
        .withColumn("log_likes", F.log1p(F.col("likes"))) \
        .withColumn("log_comment_count", F.log1p(F.col("comment_count"))) \
        .withColumn("like_ratio", F.when(F.col("views") == 0, 0.0).otherwise(F.col("likes") / F.col("views"))) \
        .withColumn("comment_ratio", F.when(F.col("views") == 0, 0.0).otherwise(F.col("comment_count") / F.col("views")))

    cols_to_keep = [
        "video_id", "parsed_trending_date", "trending_days", "log_trending_days",
        "log_views", "log_likes", "log_comment_count", "like_ratio", "comment_ratio",
        "title_length", "tag_count", "publish_hour", "publish_dow"
    ]
    
    clean_df = final_df.select([F.col(c) for c in cols_to_keep]).dropna()
    output_path = os.path.join(project_root, "data", "cleaned_youtube_regression.parquet")
    clean_df.write.mode("overwrite").parquet(output_path)
    print(f"Đã lưu Parquet sạch tại: {output_path}")
    spark.stop()

if __name__ == "__main__":
    clean_data()