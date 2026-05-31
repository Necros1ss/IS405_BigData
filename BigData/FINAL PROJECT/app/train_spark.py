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
    args = parser.parse_args()

    spark = SparkSession.builder.appName("YouTubeTrending_Orchestrator").config("spark.driver.memory", "4g").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    logger.info("Starting training pipeline at %s", datetime.now().strftime("%H:%M:%S"))

    if not os.path.exists(args.data):
        logger.error("Data not found at %s. Run python3 -m app.spark_data_cleaner first.", args.data)
        spark.stop()
        sys.exit(1)

    df_features = spark.read.parquet(args.data)
    model, _, metrics = train_spark_model(df_features, args.num_trees, args.max_depth)

    if args.save_model:
        model.write().overwrite().save(args.save_model)
        logger.info("Saved best model to %s", args.save_model)

    logger.info("Training metrics: %s", metrics)
    spark.stop()


if __name__ == "__main__":
    main()
