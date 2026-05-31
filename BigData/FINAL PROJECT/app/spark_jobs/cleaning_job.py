#!/usr/bin/env python3
"""Batch job that prepares the training parquet from the raw CSV file."""

import logging
import os

from app.spark_bootstrap import ensure_spark_runtime

ensure_spark_runtime()

from pyspark.sql import SparkSession

from app.config import CLEAN_DATA_PATH, RAW_DATA_PATH, SPARK_APP_NAME
from app.ml.target_builder import build_training_frame

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def clean_data():
    spark = (
        SparkSession.builder.appName(SPARK_APP_NAME)
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    logger.info("Loading raw dataset from %s", RAW_DATA_PATH)
    raw_df = (
        spark.read.option("header", True)
        .option("multiLine", True)
        .option("quote", '"')
        .option("escape", '"')
        .option("mode", "PERMISSIVE")
        .option("inferSchema", False)
        .csv(RAW_DATA_PATH)
    )

    logger.info("Loaded %s raw rows", f"{raw_df.count():,}")

    clean_df = build_training_frame(raw_df)
    logger.info("Prepared %s episode-level training rows", f"{clean_df.count():,}")

    if clean_df.rdd.isEmpty():
        logger.warning("No rows left after cleaning")
        spark.stop()
        return

    os.makedirs(os.path.dirname(CLEAN_DATA_PATH), exist_ok=True)
    clean_df.write.mode("overwrite").parquet(CLEAN_DATA_PATH)
    logger.info("Saved cleaned parquet to %s", CLEAN_DATA_PATH)

    spark.stop()


if __name__ == "__main__":
    clean_data()
