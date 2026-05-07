"""
Data cleaning and feature engineering for YouTube Trending data (v2 - Fixed).
Removes data leakage by eliminating like_ratio, comment_ratio.
Uses only metadata available at publish time.
"""
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType


def build_spark_session():
    """Create or get Spark session with necessary configs."""
    from pyspark.sql import SparkSession
    import os
    
    spark_local_dir = os.path.expanduser("~/spark_local")
    os.makedirs(spark_local_dir, exist_ok=True)
    
    spark = SparkSession.builder \
        .appName("YouTubeTrending_V2_Fixed") \
        .master("local[*]") \
        .config("spark.local.dir", spark_local_dir) \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark


def load_csv_with_spark(spark, path_pattern):
    """Load CSV file(s) with Spark."""
    print(f"Loading data from: {path_pattern}")
    
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("mode", "PERMISSIVE") \
        .csv(path_pattern)
    
    print(f"✓ Loaded {df.count():,} rows")
    print(f"  Columns: {df.columns}")
    
    return df


def normalize_input_columns(df):
    """Standardize column names."""
    # Mapping of possible column names to standard names
    column_mapping = {
        'video_tags': 'video_tags',
        'tags': 'video_tags',
        'like_count': 'like_count',
        'likes': 'like_count',
        'comment_count': 'comment_count',
        'comments': 'comment_count',
        'view_count': 'view_count',
        'views': 'view_count',
        'description': 'description',
        'daily_rank': 'daily_rank',
        'publish_date': 'publish_date',
        'kind': 'kind',
        'langauge': 'langauge',
        'language': 'langauge'
    }
    
    # Rename columns to standard names
    for old_col in df.columns:
        for possible_name, standard_name in column_mapping.items():
            if old_col.lower() == possible_name.lower():
                if old_col != standard_name:
                    df = df.withColumnRenamed(old_col, standard_name)
                break

    # Ensure downstream feature engineering always has the optional columns it expects.
    # Small sample files may omit publish_date or langauge.
    if "publish_date" not in df.columns:
        df = df.withColumn("publish_date", F.lit(None).cast("string"))
    if "langauge" not in df.columns:
        df = df.withColumn("langauge", F.lit(None).cast("string"))
    
    return df


def engineer_features_v2_fixed(df):
    """
    Engineer features with NO DATA LEAKAGE.
    
    Only uses metadata available at publish time:
    - title_length: Length of title (creator controls)
    - description_length: Length of description (creator controls)
    - tag_count: Number of tags (creator controls)
    - publish_hour: Hour of day published (creator controls)
    - publish_day_of_week: Day of week (creator somewhat controls)
    - is_english: Whether in English (creator controls)
    
    Target (label): daily_rank <= 50 (trending)
    
    REMOVED LEAKY FEATURES:
    - like_ratio (depends on engagement AFTER viral)
    - comment_ratio (depends on engagement AFTER viral)
    - view_count (post-viral metric)
    """
    
    print("=" * 80)
    print("FEATURE ENGINEERING V2 (Fixed - No Data Leakage)")
    print("=" * 80)
    
    # Helper function to safely convert to double
    def safe_double(column_name, default_value):
        try:
            return F.coalesce(F.col(column_name).cast(DoubleType()), F.lit(default_value))
        except:
            return F.lit(default_value)
    
    # 1. Title length
    df = df.withColumn("title_length", 
                       F.when(F.col("title").isNull(), F.lit(0))
                       .otherwise(F.length(F.col("title")).cast(DoubleType())))
    
    # 2. Description length
    df = df.withColumn("description_length", 
                       F.when(F.col("description").isNull(), F.lit(0))
                       .otherwise(F.length(F.col("description")).cast(DoubleType())))
    
    # 3. Tag count
    df = df.withColumn("tag_count", 
                       F.when(F.col("video_tags").isNull(), F.lit(0))
                       .otherwise(F.when(F.col("video_tags") == "", F.lit(0))
                                  .otherwise(F.size(F.split(F.col("video_tags"), "\\|")).cast(DoubleType())))) # Fix đếm tag chuẩn hơn
    
    # 4. Parse publish date and extract time features
    # Use SQL TRY_CAST via expr for compatibility
    df = df.withColumn("publish_date_parsed", F.expr("TRY_CAST(publish_date AS timestamp)"))
    
    df = df.withColumn("publish_hour", 
                       F.when(F.col("publish_date_parsed").isNull(), F.lit(12.0))
                       .otherwise(F.hour(F.col("publish_date_parsed")).cast(DoubleType())))
    
    df = df.withColumn("publish_day_of_week", 
                       F.when(F.col("publish_date_parsed").isNull(), F.lit(1.0))
                       .otherwise(F.dayofweek(F.col("publish_date_parsed")).cast(DoubleType())))
    
    # 5. Is English language
    df = df.withColumn("is_english",
                       F.when(F.col("langauge").isNull(), F.lit(0.0))
                       .when(F.lower(F.col("langauge")).like("%english%"), F.lit(1.0))
                       .otherwise(F.lit(0.0)))
    
    # 6. Parse rank chuẩn bị cho Deduplication (safe cast using regex)
    df = df.withColumn("daily_rank_int",
                       F.when(F.col("daily_rank").rlike("^[0-9]+$"),
                              F.col("daily_rank").cast("long"))
                        .otherwise(F.lit(None).cast("long")))
    
    print(f"\n✓ Số dòng GỐC (chứa nhiều ngày của cùng 1 video): {df.count():,}")

    # =========================================================================
    # 7. BƯỚC QUAN TRỌNG: DEDUPLICATION (CHỐNG GROUP LEAKAGE)
    # =========================================================================
    print("[Cleaning] Đang gộp các dòng trùng lặp của cùng 1 video...")
    
    # Nhóm các video lại theo `video_id` để giảm shuffle và memory pressure
    # Lấy thứ hạng (rank) NHỎ NHẤT (tốt nhất) mà video đó từng đạt được
    df_unique = df.groupBy("video_id").agg(
        F.min("daily_rank_int").alias("best_rank"),
        F.first("title_length").alias("title_length"),
        F.first("description_length").alias("description_length"),
        F.first("tag_count").alias("tag_count"),
        F.first("publish_hour").alias("publish_hour"),
        F.first("publish_day_of_week").alias("publish_day_of_week"),
        F.first("is_english").alias("is_english")
    )
    
    # =========================================================================
    # 8. TARGET: Tính Label dựa trên dataset đã được làm sạch
    # =========================================================================
    df_final = df_unique.withColumn("label",
                       F.when(F.col("best_rank").isNull(), F.lit(0.0))
                       .when(F.col("best_rank") <= 50, F.lit(1.0))
                       .otherwise(F.lit(0.0)))
    
    # Check for nulls and handle
    df_final = df_final.fillna({
        "title_length": 0,
        "description_length": 0,
        "tag_count": 0,
        "publish_hour": 12,
        "publish_day_of_week": 1,
        "is_english": 0,
        "label": 0
    })
    
    # In ra thống kê để bạn thấy rõ sự chênh lệch sau khi xóa trùng lặp
    total_unique = df_final.count()
    print(f"✓ Số video ĐỘC LẬP thực tế đem đi train: {total_unique:,}")
    
    # Target distribution
    label_counts = df_final.groupBy("label").count().collect()
    print(f"\nTarget Distribution (label = trending):")
    for row in label_counts:
        count = row['count']
        pct = (count / total_unique) * 100
        label_name = "Trending (top 50)" if row['label'] == 1 else "Not Trending"
        print(f"  {label_name}: {count:,} ({pct:.2f}%)")
    
    # Select only the features we need
    output_cols = [
        "title_length", "description_length", "tag_count",
        "publish_hour", "publish_day_of_week", "is_english",
        "label"
    ]
    
    df_out = df_final.select(output_cols)
    print(f"\n✓ Output features: {df_out.columns}")
    
    return df_out
