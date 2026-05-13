#!/usr/bin/env python3
import argparse
import os
import sys
import logging
from datetime import datetime

# Setup paths (cố định lỗi đường dẫn)
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

# IMPORT ĐÚNG TÊN FILE
try:
    from app.train_spark import train_spark_model
except ImportError as e:
    print(f"✗ Lỗi import: {e}. Vui lòng chạy lệnh từ thư mục gốc của project.")
    sys.exit(1)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--data', default="data/cleaned_youtube_regression.parquet")
    parser.add_argument('--num-trees', type=int, default=100)
    parser.add_argument('--max-depth', type=int, default=12)
    parser.add_argument('--save-model', default="models/rf_regression_model")
    args = parser.parse_args()

    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName("YouTubeTrending_Orchestrator") \
            .config("spark.driver.memory", "4g").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    logger.info(f"Bắt đầu quy trình huấn luyện: {datetime.now().strftime('%H:%M:%S')}")
    
    if not os.path.exists(args.data):
        logger.error(f"Không tìm thấy data tại {args.data}. Hãy chạy spark_data_cleaner (python3 -m app.spark_data_cleaner) trước!")
        return

    df_features = spark.read.parquet(args.data)
    model, _, _ = train_spark_model(df_features, args.num_trees, args.max_depth)

    if args.save_model:
        model.write().overwrite().save(args.save_model)
        logger.info(f"✓ Đã lưu mô hình tại: {args.save_model}")
        
    spark.stop()

if __name__ == "__main__":
    main()