#!/usr/bin/env python3

import os
import json
import logging

from app.spark_bootstrap import ensure_spark_runtime

ensure_spark_runtime()

from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator

logger = logging.getLogger(__name__)

def train_spark_model(df, num_trees=100, max_depth=12):
    # ============================================================
    # TEMPORAL SPLIT
    # ============================================================
    print("\nSplitting data:")
    print("\nTrain data:")
    print("\nTest data:")

    train_df = df.filter(F.col("trending_date_parsed") < "2026-01-01")
    test_df = df.filter(F.col("trending_date_parsed") >= "2026-01-01")

    train_count = train_df.count()
    test_count = test_df.count()

    print(f"Train rows: {train_count:,}")
    print(f"Test rows : {test_count:,}")

    if test_count == 0:
        raise Exception("Test dataset is empty. Choose another cutoff date.")

    # ============================================================
    # FEATURE PIPELINE
    # ============================================================
    feature_cols = [
        "log_views", "log_likes", "log_comment_count",
        "like_ratio", "comment_ratio", "title_length",
        "publish_hour", "publish_dow", "log_hours_to_trending"
    ]

    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features", handleInvalid="keep")

    # ============================================================
    # MODEL (Nhận tham số từ file App Nhạc Trưởng)
    # ============================================================
    rf = RandomForestRegressor(
        featuresCol="features",
        labelCol="log_trending_days",
        predictionCol="raw_prediction",
        numTrees=num_trees,   # Lấy từ Terminal
        maxDepth=max_depth,   # Lấy từ Terminal
        seed=42
    )

    pipeline = Pipeline(stages=[assembler, rf])

    print("\nTraining RandomForestRegressor...")
    model = pipeline.fit(train_df)

    # ============================================================
    # PREDICTION & CACHE
    # ============================================================
    print("Running predictions...")
    predictions = model.transform(test_df)
    predictions = predictions.withColumn("prediction", F.expr("expm1(raw_prediction)")).cache()

    # ============================================================
    # BASELINE & EVALUATION
    # ============================================================
    mean_val = train_df.select(F.avg("trending_days").alias("mean_val")).collect()[0]["mean_val"]
    predictions = predictions.withColumn("baseline_prediction", F.lit(mean_val))

    rmse_eval = RegressionEvaluator(labelCol="trending_days", predictionCol="prediction", metricName="rmse")
    mae_eval = RegressionEvaluator(labelCol="trending_days", predictionCol="prediction", metricName="mae")
    r2_eval = RegressionEvaluator(labelCol="trending_days", predictionCol="prediction", metricName="r2")

    model_rmse = rmse_eval.evaluate(predictions)
    model_mae = mae_eval.evaluate(predictions)
    model_r2 = r2_eval.evaluate(predictions)

    baseline_df = predictions.drop("prediction").withColumnRenamed("baseline_prediction", "prediction")
    baseline_rmse = rmse_eval.evaluate(baseline_df)

    print("\n" + "=" * 80)
    print("REGRESSION RESULTS")
    print("=" * 80)
    print(f"\n[BASELINE] Predict average trending days = {mean_val:.2f} | RMSE: {baseline_rmse:.4f}")
    print(f"\n[MODEL] RMSE: {model_rmse:.4f} | MAE : {model_mae:.4f} | R²  : {model_r2:.4f}")

    if model_rmse < baseline_rmse:
        print("\n✓ SUCCESS: Model outperforms baseline")
    else:
        print("\n✗ WARNING: Model worse than baseline")

    # ============================================================
    # SAVE METRICS (File app_spark sẽ lo việc lưu model)
    # ============================================================
    METRICS_PATH = "metrics/regression_metrics.json"
    metrics = {
        "rmse": float(model_rmse), "mae": float(model_mae), "r2": float(model_r2),
        "baseline_rmse": float(baseline_rmse),
        "train_rows": int(train_count), "test_rows": int(test_count),
        "features": feature_cols
    }
    os.makedirs(os.path.dirname(METRICS_PATH), exist_ok=True)
    with open(METRICS_PATH, "w") as f:
        json.dump(metrics, f, indent=4)

    return model, predictions, metrics