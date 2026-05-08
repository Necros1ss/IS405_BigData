import logging
import os
import glob
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.window import Window

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_shared_schema():
    """Schema superset for new Kaggle CSV variants.

    Keep columns as strings during ingest, then normalize/cast in one place.
    """
    return StructType([
        StructField("video_id", StringType(), True),
        StructField("trending_date", StringType(), True),
        StructField("title", StringType(), True),
        StructField("channelTitle", StringType(), True),
        StructField("channel_title", StringType(), True),
        StructField("categoryId", StringType(), True),
        StructField("category_id", StringType(), True),
        StructField("publishedAt", StringType(), True),
        StructField("publish_time", StringType(), True),
        StructField("tags", StringType(), True),
        StructField("view_count", StringType(), True),
        StructField("views", StringType(), True),
        StructField("likes", StringType(), True),
        StructField("dislikes", StringType(), True),
        StructField("comment_count", StringType(), True),
        StructField("thumbnail_link", StringType(), True),
        StructField("comments_disabled", StringType(), True),
        StructField("ratings_disabled", StringType(), True),
        StructField("video_error_or_removed", StringType(), True),
        StructField("description", StringType(), True),
        StructField("country", StringType(), True),
    ])


def _coalesce_existing(df, candidates):
    exprs = [F.col(c) for c in candidates if c in df.columns]
    if not exprs:
        return F.lit(None).cast(StringType())
    return F.coalesce(*exprs)


def normalize_input_columns(df):
    """Normalize new Kaggle column variants into one canonical schema."""
    normalized = df.select(
        _coalesce_existing(df, ["video_id"]).alias("video_id"),
        _coalesce_existing(df, ["title", "video_title"]).alias("title"),
        _coalesce_existing(df, ["channelTitle", "channel_title"]).alias("channel_title"),
        _coalesce_existing(df, ["categoryId", "category_id", "video_category_id"]).alias("category_id"),
        _coalesce_existing(df, ["publishedAt", "publish_time", "video_published_at", "channel_published_at"]).alias("publish_time"),
        _coalesce_existing(df, ["trending_date", "video_trending__date", "video_trending_date"]).alias("trending_date"),
        _coalesce_existing(df, ["view_count", "views", "video_view_count", "channel_view_count"]).alias("views"),
        _coalesce_existing(df, ["likes", "video_like_count"]).alias("likes"),
        _coalesce_existing(df, ["comment_count", "video_comment_count"]).alias("comment_count"),
        _coalesce_existing(df, ["tags", "video_tags"]).alias("tags"),
        _coalesce_existing(df, ["description", "video_description", "channel_description"]).alias("description"),
        _coalesce_existing(df, ["country", "video_trending_country", "channel_country", "country_from_file"]).alias("country"),
    )

    normalized = normalized \
        .withColumn("views", F.col("views").cast(DoubleType())) \
        .withColumn("likes", F.col("likes").cast(DoubleType())) \
        .withColumn("comment_count", F.col("comment_count").cast(DoubleType())) \
        .fillna({
            "title": "",
            "channel_title": "",
            "category_id": "",
            "publish_time": "",
            "trending_date": "",
            "tags": "",
            "description": "",
            "country": "UNKNOWN",
            "views": 0.0,
            "likes": 0.0,
            "comment_count": 0.0,
        })

    return normalized


def parse_trending_date(df):
    cleaned_date = F.regexp_replace(F.trim(F.col("trending_date")), r"[./]", "-")
    return df.withColumn(
        "parsed_trending_date",
        F.coalesce(
            F.to_date(cleaned_date, "yyyy-MM-dd"),
            F.to_date(cleaned_date, "yy-MM-dd"),
            F.to_date(F.to_timestamp(cleaned_date, "yyyy-MM-dd HH:mm:ss")),
            F.to_date(F.to_timestamp(cleaned_date, "yyyy-MM-dd'T'HH:mm:ssX")),
        ),
    )


def apply_shared_feature_engineering(df):
    """Shared feature engineering used by both batch and streaming."""
    df = df.withColumn("title_length", F.length(F.col("title")).cast(DoubleType())) \
        .withColumn("description_length", F.length(F.col("description")).cast(DoubleType())) \
        .withColumn(
            "tag_count",
            F.when(F.col("tags") == "", F.lit(0.0))
            .otherwise(F.size(F.split(F.col("tags"), "\\|")).cast(DoubleType())),
        )

    df = df.withColumn(
        "publish_ts",
        F.coalesce(
            F.to_timestamp("publish_time"),
            F.to_timestamp("publish_time", "yyyy-MM-dd'T'HH:mm:ssX"),
            F.to_timestamp("publish_time", "yyyy-MM-dd HH:mm:ss"),
        ),
    ) \
        .withColumn(
            "publish_hour",
            F.when(F.col("publish_ts").isNull(), F.lit(12.0))
            .otherwise(F.hour(F.col("publish_ts")).cast(DoubleType())),
        ) \
        .withColumn(
            "publish_day_of_week",
            F.when(F.col("publish_ts").isNull(), F.lit(1.0))
            .otherwise(F.dayofweek(F.col("publish_ts")).cast(DoubleType())),
        )

    df = df.withColumn("log_views", F.log1p(F.col("views"))) \
        .withColumn("log_likes", F.log1p(F.col("likes"))) \
        .withColumn("log_comment_count", F.log1p(F.col("comment_count"))) \
        .withColumn(
            "like_ratio",
            F.when(F.col("views") <= 0, F.lit(0.0)).otherwise(F.col("likes") / F.col("views")),
        ) \
        .withColumn(
            "comment_ratio",
            F.when(F.col("views") <= 0, F.lit(0.0)).otherwise(F.col("comment_count") / F.col("views")),
        )

    return df


def clean_data():
    logger.info("Khởi tạo Spark Session cho ETL Pipeline...")
    spark = SparkSession.builder \
        .appName("YouTubeTrending_ETL_NewKaggle") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "20") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_path = os.path.join(project_root, "data", "*_youtube_trending_data.csv")
    input_paths = sorted(glob.glob(data_path))

    logger.info(f"Đọc dữ liệu từ: {data_path}")
    raw_schema = get_shared_schema()

    if not input_paths:
        raise FileNotFoundError(
            "Khong tim thay file *_youtube_trending_data.csv trong folder data/. "
            "Hay download dataset canerkonuk/youtube-trending-videos-global truoc."
        )

    df = spark.read.option("header", "true") \
        .option("multiLine", "true") \
        .option("escape", '"') \
        .csv(input_paths)

    # Infer country from filename when the CSV itself does not contain country.
    df = df.withColumn("file_name", F.input_file_name()) \
        .withColumn("country_from_file", F.regexp_extract(F.col("file_name"), r"([A-Z]{2})_youtube_trending_data\.csv", 1)) \
        .drop("file_name")

    df = normalize_input_columns(df)

    df = parse_trending_date(df)
    df = df.filter(F.col("video_id").isNotNull() & F.col("parsed_trending_date").isNotNull())
    df = df.filter(F.year(F.col("parsed_trending_date")) >= 2024)

    logger.info("Tinh target trending_days = countDistinct(trending_date) theo video_id...")
    target_df = df.groupBy("video_id").agg(
        F.countDistinct("parsed_trending_date").cast(DoubleType()).alias("trending_days")
    )

    logger.info("Lay earliest trending snapshot de tranh data leakage...")
    window_spec = Window.partitionBy("video_id").orderBy(F.col("parsed_trending_date").asc())
    early_snapshot_df = df.withColumn("row_num", F.row_number().over(window_spec)) \
        .filter(F.col("row_num") == 1) \
        .drop("row_num")

    final_df = early_snapshot_df.join(target_df, on="video_id", how="inner")
    final_df = apply_shared_feature_engineering(final_df)

    duplicate_videos = final_df.groupBy("video_id").count().filter(F.col("count") > 1).count()
    if duplicate_videos > 0:
        raise ValueError("LEAKAGE ALERT: Duplicate snapshot rows after earliest-snapshot selection.")

    cols_to_keep = [
        "video_id", "parsed_trending_date", "trending_days", "country",
        "log_views", "log_likes", "log_comment_count", "like_ratio", "comment_ratio",
            "title_length", "description_length", "tag_count",
        "publish_hour", "publish_day_of_week",
    ]

    clean_df = final_df.select([F.col(c) for c in cols_to_keep]).dropna(subset=[
        "video_id", "parsed_trending_date", "trending_days",
        "log_views", "log_likes", "log_comment_count", "like_ratio", "comment_ratio",
        "title_length", "description_length", "tag_count", "publish_hour", "publish_day_of_week",
    ])

    output_path = os.path.join(project_root, "data", "cleaned_youtube_regression.parquet")
    logger.info("Ghi parquet output partition theo country, publish_day_of_week...")
    clean_df.write.mode("overwrite").partitionBy("country", "publish_day_of_week").parquet(output_path)
    logger.info(f"Hoan thanh ETL. Output: {output_path}")
    spark.stop()

if __name__ == "__main__":
    clean_data()