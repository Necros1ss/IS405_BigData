#!/usr/bin/env python3
"""Train the regression models from the cleaned parquet dataset."""

import argparse
import logging
import os
import sys
from datetime import datetime

from app.spark_bootstrap import ensure_spark_runtime

ensure_spark_runtime()

from pyspark.sql import SparkSession

from app.ml.training_pipeline import train_spark_model

__all__ = ["train_spark_model"]

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", default="data/cleaned_youtube_regression.parquet")
    parser.add_argument("--num-trees", type=int, default=100)
    parser.add_argument("--max-depth", type=int, default=12)
    parser.add_argument("--save-model", default="models/rf_regression_model")
    parser.add_argument(
        "--model",
        default="all",
        choices=["all", "linear_regression", "gradient_boosting", "random_forest", "random_forest_tuned"],
        help="Train one model at a time, or all models in one run",
    )
    parser.add_argument("--no-tune", action="store_true", help="Skip cross-validation tuning for a faster run")
    parser.add_argument("--fast", action="store_true", help="Train only the RandomForest path for constrained environments")
    parser.add_argument("--ultra-fast", action="store_true", help="Train only the Linear Regression path for the most constrained environments")
    parser.add_argument("--model-output-dir", default="models", help="Directory used to persist per-algorithm model artifacts")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("YouTubeTrending_Orchestrator").config("spark.driver.memory", "4g").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    logger.info("Starting training pipeline at %s", datetime.now().strftime("%H:%M:%S"))

    if not os.path.exists(args.data):
        logger.error("Data not found at %s. Run python3 -m app.spark_data_cleaner first.", args.data)
        spark.stop()
        sys.exit(1)

    df_features = spark.read.parquet(args.data)
    os.environ["TRAIN_MODEL"] = args.model
    if args.fast:
        os.environ["TRAIN_FAST_MODE"] = "1"
        os.environ["DISABLE_OPTIONAL_MODELS"] = "1"
    if args.ultra_fast:
        os.environ["TRAIN_ULTRA_FAST_MODE"] = "1"
        os.environ["TRAIN_FAST_MODE"] = "1"
        os.environ["DISABLE_OPTIONAL_MODELS"] = "1"
    os.environ["MODEL_OUTPUT_DIR"] = args.model_output_dir
    model, _, metrics = train_spark_model(df_features, args.num_trees, args.max_depth, tune=not args.no_tune)

    if model is None:
        logger.error("Training did not produce a usable model for %s", args.model)
        spark.stop()
        sys.exit(1)

    if args.save_model:
        model.write().overwrite().save(args.save_model)
        logger.info("Saved best model to %s", args.save_model)

    logger.info("Training metrics: %s", metrics)
    spark.stop()


if __name__ == "__main__":
    main()
