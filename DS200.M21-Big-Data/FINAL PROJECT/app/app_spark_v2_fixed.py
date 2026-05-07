#!/usr/bin/env python3
import argparse
import os
from datetime import datetime
from pyspark.sql import SparkSession
from app.train_spark import train_spark_model

def build_spark_session(app_name="YouTubeTrending_Train"):
    spark = SparkSession.builder.appName(app_name).config("spark.driver.memory", "4g").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--data', '-d', default="data/cleaned_youtube_regression.parquet")
    parser.add_argument('--num-trees', type=int, default=100)
    parser.add_argument('--max-depth', type=int, default=12)
    parser.add_argument('--save-model', default="models/rf_regression_model")
    args = parser.parse_args()

    spark = build_spark_session()
    print("\n[Loading Data...]")
    if not os.path.exists(args.data):
        print(f"✗ Data not found: {args.data}. Run clean_spark.py first.")
        return

    df_features = spark.read.parquet(args.data)
    model, predictions, metrics = train_spark_model(df_features, args.num_trees, args.max_depth)

    if args.save_model:
        model.write().overwrite().save(args.save_model)
        print(f"\n✓ Model saved to: {args.save_model}")
    spark.stop()

if __name__ == "__main__":
    main()