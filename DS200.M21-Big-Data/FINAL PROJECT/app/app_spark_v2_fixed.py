#!/usr/bin/env python3
"""
YouTube Trending - PySpark pipeline V2 (FIXED - No Data Leakage)
Reads large CSV(s) with Spark, engineers REAL features (no leakage),
trains a Spark ML RandomForest, and generates evaluation metrics.

Key Fixes:
- Removed like_ratio, comment_ratio (pure leakage)
- Added title_length, publish_hour, publish_day_of_week
- Uses only metadata available at publish time
- Target: daily_rank <= 50 (trending)

Usage:
  python3 app/app_spark_v2_fixed.py --data <path_to_csv> --save-model <model_path>

Expected Accuracy: 70%-85% (realistic, not 100%)
"""
import sys
import os
import argparse
import json
from datetime import datetime


def _append_local_spark_paths():
    """Add local Spark paths to sys.path if needed."""
    spark_home = os.environ.get("SPARK_HOME")
    if not spark_home:
        for candidate in ("/home/thinh/spark", os.path.expanduser("~/spark")):
            if os.path.isdir(candidate):
                spark_home = candidate
                os.environ["SPARK_HOME"] = candidate
                break

    if not spark_home:
        return

    spark_python = os.path.join(spark_home, "python")
    spark_pyspark_zip = os.path.join(spark_home, "python", "lib", "pyspark.zip")
    spark_py4j = os.path.join(spark_home, "python", "lib", "py4j-0.10.9.9-src.zip")

    for path in (spark_python, spark_pyspark_zip, spark_py4j):
        if os.path.exists(path) and path not in sys.path:
            sys.path.insert(0, path)


# Import fixed modules
try:
    from app.clean_spark_v2_fixed import (
        build_spark_session, 
        load_csv_with_spark, 
        normalize_input_columns, 
        engineer_features_v2_fixed
    )
    from app.train_spark_v2_fixed import train_spark_model_v2, prepare_data_v2_no_leakage
except ImportError as e:
    print(f"✗ Import error: {e}")
    _append_local_spark_paths()
    try:
        from app.clean_spark_v2_fixed import (
            build_spark_session, 
            load_csv_with_spark, 
            normalize_input_columns, 
            engineer_features_v2_fixed
        )
        from app.train_spark_v2_fixed import train_spark_model_v2, prepare_data_v2_no_leakage
    except ImportError as e2:
        print(f"✗ Failed to import modules: {e2}")
        print("Ensure app/ directory contains clean_spark_v2_fixed.py and train_spark_v2_fixed.py")
        sys.exit(1)


def main():
    """Main orchestration function (V2 Fixed)."""
    parser = argparse.ArgumentParser(
        description="YouTube Trending Spark ML Pipeline V2 (Fixed - No Data Leakage)"
    )
    parser.add_argument('--data', '-d', help='Path pattern to CSV(s) or folder', default=None)
    parser.add_argument('--sample-fraction', type=float, default=0.01,
                        help='Fraction to sample from train/test (0-1). Set 1.0 for full.')
    parser.add_argument('--no-sample', action='store_true', help='Disable sampling and train on full split')
    parser.add_argument('--num-trees', type=int, default=100, help='Number of trees for RandomForest')
    parser.add_argument('--max-depth', type=int, default=12, help='Max depth for RandomForest')
    parser.add_argument('--save-model', dest='save_model', help='Path to save trained model', default=None)
    parser.add_argument('--save-metrics', dest='save_metrics', help='Local path to save metrics JSON', default=None)
    
    args = parser.parse_args()

    print("\n" + "=" * 100)
    print("YOUTUBE TRENDING - SPARK ML PIPELINE V2 (FIXED - NO DATA LEAKAGE)")
    print("=" * 100)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 100)

    # Build Spark session
    print("\n[Initializing Spark...]")
    spark = build_spark_session()
    print(f"✓ Spark session created")

    # Determine data path
    if args.data:
        path_pattern = args.data
    else:
        # Try default paths
        local_csv = "kaggle_youtube/trending_yt_videos_113_countries.csv"
        if os.path.exists(local_csv):
            path_pattern = local_csv
        else:
            print('✗ No data path provided and default not found.')
            print('Provide --data <path>')
            spark.stop()
            return

    # Stage 1: Data loading & cleaning
    print("\n" + "=" * 100)
    print("STAGE 1: DATA LOADING & CLEANING")
    print("=" * 100)
    
    df_raw = load_csv_with_spark(spark, path_pattern)
    df_normalized = normalize_input_columns(df_raw)
    print(f"✓ Normalized {df_raw.count():,} rows")
    
    # Stage 2: Feature engineering (V2 Fixed - no leakage)
    print("\n" + "=" * 100)
    print("STAGE 2: FEATURE ENGINEERING (V2 - FIXED)")
    print("=" * 100)
    
    df_features = engineer_features_v2_fixed(df_normalized)
    
    # Additional preparation
    df_prepared = prepare_data_v2_no_leakage(df_features)
    
    # Stage 3: Model training
    print("\n" + "=" * 100)
    print("STAGE 3: MODEL TRAINING")
    print("=" * 100)
    
    model, predictions, metrics = train_spark_model_v2(
        df_prepared,
        sample_fraction=args.sample_fraction,
        no_sample=args.no_sample,
        num_trees=args.num_trees,
        max_depth=args.max_depth
    )

    # Stage 4: Save model
    if args.save_model:
        print("\n" + "=" * 100)
        print("STAGE 4: SAVING MODEL")
        print("=" * 100)
        try:
            print(f"Saving model to: {args.save_model}")
            model.write().overwrite().save(args.save_model)
            print(f"✓ Model saved successfully")
            print(f"  Location: {args.save_model}")
        except Exception as e:
            print(f"✗ Error saving model: {e}")

    # Stage 5: Save metrics
    if args.save_metrics:
        print("\n" + "=" * 100)
        print("STAGE 5: SAVING METRICS")
        print("=" * 100)
        try:
            print(f"Saving metrics to: {args.save_metrics}")
            os.makedirs(os.path.dirname(args.save_metrics) or ".", exist_ok=True)
            with open(args.save_metrics, 'w') as f:
                json.dump(metrics, f, indent=2)
            print(f"✓ Metrics saved successfully")
        except Exception as e:
            print(f"✗ Error saving metrics: {e}")

    # Final summary
    print("\n" + "=" * 100)
    print("SUMMARY")
    print("=" * 100)
    print(f"\nKey Metrics:")
    print(f"  AUC: {metrics.get('auc', 'N/A'):.4f}")
    print(f"  Accuracy: {metrics.get('accuracy', 'N/A'):.4f}")
    print(f"  F1-Score: {metrics.get('f1', 'N/A'):.4f}")
    
    tp = metrics.get('tp', 0)
    tn = metrics.get('tn', 0)
    fp = metrics.get('fp', 0)
    fn = metrics.get('fn', 0)
    
    if tp + tn + fp + fn > 0:
        total_correct = tp + tn
        total = tp + tn + fp + fn
        print(f"\nConfusion Matrix:")
        print(f"  True Positives: {tp} (correctly identified trending)")
        print(f"  True Negatives: {tn} (correctly identified non-trending)")
        print(f"  False Positives: {fp} (false alarms)")
        print(f"  False Negatives: {fn} (missed trending)")
        print(f"  Total Correct: {total_correct}/{total}")
    
    if metrics.get('feature_importances'):
        print(f"\nTop Feature Importances:")
        importances = sorted(
            metrics['feature_importances'].items(),
            key=lambda x: x[1],
            reverse=True
        )
        for i, (name, importance) in enumerate(importances[:5], 1):
            print(f"  {i}. {name}: {importance:.4f} ({importance*100:.2f}%)")

    print("\n" + "=" * 100)
    print(f"Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 100)
    
    spark.stop()


if __name__ == "__main__":
    main()
