#!/usr/bin/env python3
"""
YouTube Trending - PySpark pipeline Regression
Reads cleaned Parquet dataset, trains a Spark ML RandomForestRegressor, 
and generates evaluation metrics (RMSE, MAE, R2).

Usage:
  python3 app/app_spark_regression.py --data data/cleaned_youtube_regression.parquet --save-model models/rf_regression_model
"""
import sys
import os
import argparse
import json
from datetime import datetime
from pyspark.sql import SparkSession

def build_spark_session(app_name="YouTubeTrending_Regression_Train"):
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark

try:
    from app.train_spark_regression import train_spark_model_regression, prepare_data_regression
except ImportError as e:
    print(f"✗ Import error: {e}. Make sure app/train_spark_regression.py exists.")
    sys.exit(1)

def main():
    parser = argparse.ArgumentParser(description="YouTube Trending Spark ML Pipeline (Regression)")
    parser.add_argument('--data', '-d', help='Path to cleaned parquet data', default="data/cleaned_youtube_regression.parquet")
    parser.add_argument('--sample-fraction', type=float, default=1.0, help='Fraction to sample (0-1).')
    parser.add_argument('--no-sample', action='store_true', default=True, help='Train on full split')
    parser.add_argument('--num-trees', type=int, default=100, help='Number of trees for RandomForest')
    parser.add_argument('--max-depth', type=int, default=12, help='Max depth for RandomForest')
    parser.add_argument('--save-model', dest='save_model', help='Path to save trained model', default=None)
    parser.add_argument('--save-metrics', dest='save_metrics', help='Local path to save metrics JSON', default=None)
    
    args = parser.parse_args()

    print("\n" + "=" * 100)
    print("YOUTUBE TRENDING - SPARK ML PIPELINE (REGRESSION)")
    print("=" * 100)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    print("\n[Initializing Spark...]")
    spark = build_spark_session()
    print(f"✓ Spark session created")

    if not os.path.exists(args.data):
        print(f"✗ Data file not found: {args.data}")
        print("Please run clean_spark_regression.py first to generate the dataset.")
        spark.stop()
        return

    # Load Cleaned Data
    print("\n" + "=" * 100)
    print("STAGE 1: LOADING PREPARED DATA")
    print("=" * 100)
    
    df_features = spark.read.parquet(args.data)
    print(f"✓ Loaded {df_features.count():,} rows from {args.data}")
    
    df_prepared = prepare_data_regression(df_features)
    
    # Model Training
    print("\n" + "=" * 100)
    print("STAGE 2: MODEL TRAINING")
    print("=" * 100)
    
    model, predictions, metrics = train_spark_model_regression(
        df_prepared,
        sample_fraction=args.sample_fraction,
        no_sample=args.no_sample,
        num_trees=args.num_trees,
        max_depth=args.max_depth
    )

    # Save model
    if args.save_model:
        print("\n" + "=" * 100)
        print("STAGE 3: SAVING MODEL")
        print("=" * 100)
        try:
            model.write().overwrite().save(args.save_model)
            print(f"✓ Model saved successfully to: {args.save_model}")
        except Exception as e:
            print(f"✗ Error saving model: {e}")

    # Final summary
    print("\n" + "=" * 100)
    print("SUMMARY (REGRESSION)")
    print("=" * 100)
    print(f"\nKey Metrics:")
    print(f"  RMSE (Error in days): {metrics.get('rmse', 'N/A'):.4f}")
    print(f"  MAE (Absolute Error): {metrics.get('mae', 'N/A'):.4f}")
    print(f"  R² (Variance Explained): {metrics.get('r2', 'N/A'):.4f}")
    
    if metrics.get('feature_importances'):
        print(f"\nTop Feature Importances:")
        importances = sorted(metrics['feature_importances'].items(), key=lambda x: x[1], reverse=True)
        for i, (name, imp) in enumerate(importances[:5], 1):
            print(f"  {i}. {name}: {imp:.4f} ({imp*100:.2f}%)")

    print("\n" + "=" * 100)
    print(f"Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 100)
    
    spark.stop()

if __name__ == "__main__":
    main()