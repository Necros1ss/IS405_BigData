#!/usr/bin/env python3
"""
YouTube Trending - PySpark pipeline
Reads large CSV(s) with Spark, engineers features, trains a Spark ML RandomForest,
and makes sample predictions.

Usage:
  python3 app/app_spark.py --data <path_to_extracted_csv_folder_or_file>

If PySpark is not available, script prints instructions and exits.
"""
import sys
import os
import argparse

def _append_local_spark_paths():
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


from app.clean_spark import build_spark_session, load_csv_with_spark, normalize_input_columns, engineer_features
from app.train_spark import train_spark_model
from app.predict_spark import predict_sample


# This script is now a thin orchestrator that imports modular helpers from
# app.clean_spark, app.train_spark and app.predict_spark.


def train_spark_model(df_features, sample_fraction=0.01, no_sample=False, num_trees=10, max_depth=6):
    # Create label: trending if engagement > median
    median_val = df_features.approxQuantile("engagement", [0.5], 0.01)[0]
    print(f"Median engagement (approx): {median_val}")
    df_labeled = df_features.withColumn("label", F.when(F.col("engagement") > F.lit(median_val), 1).otherwise(0))

    feature_cols = ['tag_count', 'description_length', 'like_ratio', 'comment_ratio', 'engagement']
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

    # Split and train
    train, test = df_labeled.randomSplit([0.8, 0.2], seed=42)

    if not no_sample and sample_fraction is not None and 0 < sample_fraction < 1.0:
        train = train.sample(False, float(sample_fraction), seed=42)
        test = test.sample(False, float(sample_fraction), seed=42)
        print(f"Training on {train.count()} sampled rows, testing on {test.count()} sampled rows (fraction={sample_fraction})")
    else:
        print(f"Training on {train.count()} rows, testing on {test.count()} rows (no sampling)")

    rf = RandomForestClassifier(featuresCol="features", labelCol="label", numTrees=int(num_trees), maxDepth=int(max_depth))
    pipeline = Pipeline(stages=[assembler, rf])

    model = pipeline.fit(train)

    # Evaluate
    predictions = model.transform(test)
    evaluator = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC")
    auc = evaluator.evaluate(predictions)
    print(f"Model AUC on test set: {auc:.4f}")

    # Feature importances from RF stage
    rf_model = model.stages[-1]
    metrics = {"auc": float(auc)}
    try:
        importances = rf_model.featureImportances
        print("Feature importances:")
        imp_dict = {}
        for name, imp in zip(feature_cols, importances):
            print(f"  {name}: {imp:.4f}")
            imp_dict[name] = float(imp)
        metrics["feature_importances"] = imp_dict
    except Exception:
        pass

    return model, predictions, metrics


def predict_sample(model):
    spark = model.stages[0].transform  # hack to get spark from pipeline stage (not needed)
    # Create small sample DataFrame using Spark
    from pyspark.sql import Row
    samples = [Row(tag_count=40, description_length=800, like_ratio=0.10, comment_ratio=0.05, engagement=50000),
               Row(tag_count=5, description_length=100, like_ratio=0.01, comment_ratio=0.001, engagement=100),
               Row(tag_count=25, description_length=400, like_ratio=0.08, comment_ratio=0.03, engagement=20000)]
    spark_sess = SparkSession.builder.getOrCreate()
    df_new = spark_sess.createDataFrame(samples)
    assembler = model.stages[0]
    rf = model.stages[-1]
    df_feat = assembler.transform(df_new)
    preds = model.transform(df_new)
    preds.select("tag_count", "description_length", "like_ratio", "comment_ratio", "engagement", "prediction", "probability").show(truncate=False)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--data', '-d', help='Path pattern to CSV(s) or folder', default=None)
    parser.add_argument('--sample-fraction', type=float, default=0.01,
                        help='Fraction to sample from train/test after split (0-1). Set 1.0 for full.)')
    parser.add_argument('--no-sample', action='store_true', help='Disable sampling and train on full split')
    parser.add_argument('--num-trees', type=int, default=10, help='Number of trees for RandomForest')
    parser.add_argument('--max-depth', type=int, default=6, help='Max depth for RandomForest')
    parser.add_argument('--save-model', dest='save_model', help='HDFS or local path to save trained PipelineModel', default=None)
    parser.add_argument('--save-predictions', dest='save_predictions', help='HDFS or local path to save predictions (Parquet)', default=None)
    parser.add_argument('--save-predictions-csv', dest='save_predictions_csv', help='HDFS or local path to save predictions as CSV', default=None)
    parser.add_argument('--save-metrics', dest='save_metrics', help='Local or HDFS path to save model metrics (JSON)', default=None)
    args = parser.parse_args()

    spark = build_spark_session()

    # determine data path
    if args.data:
        path_pattern = args.data
    else:
        kaggle_cache = os.path.expanduser('~/.cache/kagglehub/datasets/asaniczka/trending-youtube-videos-113-countries')
        if os.path.exists(kaggle_cache):
            path_pattern = os.path.join(kaggle_cache, '*.csv')
        else:
            print('No data path provided and Kaggle cache not found. Provide --data <path> or extract dataset to cache.')
            spark.stop()
            return

    df_raw = normalize_input_columns(load_csv_with_spark(spark, path_pattern))
    print(f"Normalized columns: {df_raw.columns}")
    df_features = engineer_features(df_raw)
    model, predictions, metrics = train_spark_model(df_features,
                              sample_fraction=args.sample_fraction,
                              no_sample=args.no_sample,
                              num_trees=args.num_trees,
                              max_depth=args.max_depth)

    # optional saves
    if args.save_model:
        try:
            print(f"Saving model to {args.save_model}")
            model.write().overwrite().save(args.save_model)
            print("Model saved successfully")
        except Exception as e:
            print("Error saving model:", e)

    if args.save_predictions:
        try:
            print(f"Saving predictions to {args.save_predictions} (Parquet)")
            predictions.write.mode("overwrite").parquet(args.save_predictions)
            print("Predictions saved successfully")
        except Exception as e:
            print("Error saving predictions:", e)

    if args.save_predictions_csv:
        try:
            print(f"Saving predictions to {args.save_predictions_csv} (CSV)")
            predictions.coalesce(1).write.mode("overwrite").option("header", "true").csv(args.save_predictions_csv)
            print("Predictions (CSV) saved successfully")
        except Exception as e:
            print("Error saving predictions as CSV:", e)

    if args.save_metrics:
        try:
            import json
            print(f"Saving metrics to {args.save_metrics}")
            metrics_json = json.dumps(metrics, indent=2)
            with open(args.save_metrics, 'w') as f:
                f.write(metrics_json)
            print("Metrics saved successfully")
        except Exception as e:
            print("Error saving metrics:", e)

    predict_sample(model)

    spark.stop()

if __name__ == '__main__':
    main()
