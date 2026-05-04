#!/usr/bin/env python3
"""
YouTube Trending - PySpark pipeline (Orchestrator)
Reads large CSV(s) with Spark, engineers features, trains a Spark ML RandomForest,
makes predictions, and generates visualizations.

This is a thin orchestrator that imports modular helpers from:
- app.clean_spark: data loading & feature engineering
- app.train_spark: model training
- app.predict_spark: sample predictions
- app.visualize_spark: visualization & metrics export

Usage:
  python3 app/app_spark.py --data <path_to_csv> [--save-visualizations Images]

If PySpark is not available, script prints instructions and exits.
"""
import sys
import os
import argparse

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


# Import modular helpers
try:
    from app.clean_spark import build_spark_session, load_csv_with_spark, normalize_input_columns, engineer_features
    from app.train_spark import train_spark_model
    from app.predict_spark import predict_sample
    from app.visualize_spark import generate_all_visualizations
except ImportError as e:
    print(f"✗ Import error: {e}")
    _append_local_spark_paths()
    try:
        from app.clean_spark import build_spark_session, load_csv_with_spark, normalize_input_columns, engineer_features
        from app.train_spark import train_spark_model
        from app.predict_spark import predict_sample
        from app.visualize_spark import generate_all_visualizations
    except ImportError as e2:
        print(f"✗ Failed to import modules: {e2}")
        print("Ensure app/ directory contains clean_spark.py, train_spark.py, predict_spark.py, and visualize_spark.py")
        sys.exit(1)


def main():
    """Main orchestration function."""
    parser = argparse.ArgumentParser(description="YouTube Trending Spark ML Pipeline")
    parser.add_argument('--data', '-d', help='Path pattern to CSV(s) or folder', default=None)
    parser.add_argument('--sample-fraction', type=float, default=0.01,
                        help='Fraction to sample from train/test after split (0-1). Set 1.0 for full.')
    parser.add_argument('--no-sample', action='store_true', help='Disable sampling and train on full split')
    parser.add_argument('--num-trees', type=int, default=10, help='Number of trees for RandomForest')
    parser.add_argument('--max-depth', type=int, default=6, help='Max depth for RandomForest')
    parser.add_argument('--save-model', dest='save_model', help='HDFS or local path to save trained PipelineModel', default=None)
    parser.add_argument('--save-predictions', dest='save_predictions', help='HDFS or local path to save predictions (Parquet)', default=None)
    parser.add_argument('--save-predictions-csv', dest='save_predictions_csv', help='HDFS or local path to save predictions as CSV', default=None)
    parser.add_argument('--save-metrics', dest='save_metrics', help='Local path to save model metrics (JSON)', default=None)
    parser.add_argument('--save-visualizations', dest='save_visualizations', help='Directory to save visualization plots', default="Images")
    parser.add_argument('--no-visualizations', action='store_true', help='Disable visualization generation')
    
    args = parser.parse_args()

    # Build Spark session
    spark = build_spark_session()

    # Determine data path
    if args.data:
        path_pattern = args.data
    else:
        kaggle_cache = os.path.expanduser('~/.cache/kagglehub/datasets/asaniczka/trending-youtube-videos-113-countries')
        if os.path.exists(kaggle_cache):
            path_pattern = os.path.join(kaggle_cache, '*.csv')
        else:
            print('✗ No data path provided and Kaggle cache not found.')
            print('Provide --data <path> or extract dataset to cache.')
            spark.stop()
            return

    # Data loading & cleaning
    print("\n=== Stage 1: Data Loading & Cleaning ===")
    df_raw = load_csv_with_spark(spark, path_pattern)
    df_normalized = normalize_input_columns(df_raw)
    print(f"✓ Normalized columns: {df_normalized.columns}")
    
    # Feature engineering
    print("\n=== Stage 2: Feature Engineering ===")
    df_features = engineer_features(df_normalized)
    print(f"✓ Engineered features: {df_features.columns}")
    
    # Model training
    print("\n=== Stage 3: Model Training ===")
    model, predictions, metrics = train_spark_model(
        df_features,
        sample_fraction=args.sample_fraction,
        no_sample=args.no_sample,
        num_trees=args.num_trees,
        max_depth=args.max_depth
    )

    # Save model
    if args.save_model:
        try:
            print(f"\n=== Stage 4: Saving Model ===")
            print(f"Saving model to {args.save_model}")
            model.write().overwrite().save(args.save_model)
            print("✓ Model saved successfully")
        except Exception as e:
            print(f"✗ Error saving model: {e}")

    # Save predictions (Parquet)
    if args.save_predictions:
        try:
            print(f"\nSaving predictions to {args.save_predictions} (Parquet)")
            predictions.write.mode("overwrite").parquet(args.save_predictions)
            print("✓ Predictions saved successfully")
        except Exception as e:
            print(f"✗ Error saving predictions: {e}")

    # Save predictions (CSV)
    if args.save_predictions_csv:
        try:
            print(f"Saving predictions to {args.save_predictions_csv} (CSV)")
            predictions.coalesce(1).write.mode("overwrite").option("header", "true").csv(args.save_predictions_csv)
            print("✓ Predictions (CSV) saved successfully")
        except Exception as e:
            print(f"✗ Error saving predictions as CSV: {e}")

    # Save metrics (JSON)
    if args.save_metrics:
        try:
            import json
            print(f"Saving metrics to {args.save_metrics}")
            metrics_json = json.dumps(metrics, indent=2)
            with open(args.save_metrics, 'w') as f:
                f.write(metrics_json)
            print("✓ Metrics saved successfully")
        except Exception as e:
            print(f"✗ Error saving metrics: {e}")

    # Generate visualizations
    if not args.no_visualizations:
        try:
            print(f"\n=== Stage 5: Generating Visualizations ===")
            generate_all_visualizations(
                metrics,
                df_features=df_features,
                output_dir=args.save_visualizations
            )
            print(f"✓ Visualizations saved to {args.save_visualizations}/")
        except Exception as e:
            print(f"⚠ Warning: Visualization generation failed: {e}")
            print("  (This is non-critical; training completed successfully)")

    # Make sample predictions
    print(f"\n=== Stage 6: Sample Predictions ===")
    predict_sample(model)

    print("\n✓ Pipeline completed successfully!")
    spark.stop()


if __name__ == '__main__':
    main()
