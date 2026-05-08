import logging
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator

logger = logging.getLogger(__name__)

def train_spark_model(df_features, num_trees=100, max_depth=12):
    logger.info("BAT DAU HUAN LUYEN MO HINH (Multi-Country + Temporal Split)")
    
    country_indexer = StringIndexer(inputCol="country", outputCol="country_index", handleInvalid="keep")

    feature_cols = [
        'country_index',
        'log_views', 'log_likes', 'log_comment_count', 
        'like_ratio', 'comment_ratio',
        'title_length', 'description_length', 'tag_count',
        'publish_hour', 'publish_day_of_week',
    ]
    
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features", handleInvalid="keep")
    
    # Time-based split (no random split).
    ts_df = df_features.select(
        F.unix_timestamp(F.col("parsed_trending_date").cast("timestamp")).alias("ts")
    ).na.drop()
    quantiles = ts_df.approxQuantile("ts", [0.8], 0.01)
    cutoff_ts = quantiles[0] if quantiles and quantiles[0] is not None else None
    if cutoff_ts is None:
        raise ValueError("Khong the tinh cutoff date tu parsed_trending_date")

    cutoff_date = datetime.utcfromtimestamp(cutoff_ts).strftime("%Y-%m-%d")
    train = df_features.filter(F.col("parsed_trending_date") < cutoff_date)
    test = df_features.filter(F.col("parsed_trending_date") >= cutoff_date)

    if train.rdd.isEmpty() or test.rdd.isEmpty():
        logger.warning("Temporal split rong; fallback sang randomSplit(0.8, 0.2) de tiep tuc huan luyen.")
        train, test = df_features.randomSplit([0.8, 0.2], seed=42)
        cutoff_date = "random-split"
        if train.rdd.isEmpty() or test.rdd.isEmpty():
            raise ValueError("Khong the tao train/test split hop le tu du lieu hien tai.")
    
    logger.info(f"Tập Huấn Luyện (Trước {cutoff_date}): {train.count():,} videos")
    logger.info(f"Tập Kiểm Thử (Sau {cutoff_date}): {test.count():,} videos")
    
    rf = RandomForestRegressor(
        featuresCol="features", 
        labelCol="trending_days",
        numTrees=int(num_trees), 
        maxDepth=int(max_depth),
        seed=42
    )
    
    pipeline = Pipeline(stages=[country_indexer, assembler, rf])
    logger.info("Đang Fit Model...")
    model = pipeline.fit(train)

    predictions = model.transform(test)
    predictions = predictions.withColumn("predicted_days", F.greatest(F.col("prediction"), F.lit(0.0)))
    
    mean_val = train.select(F.avg("trending_days").alias("mean_val")).collect()[0]["mean_val"]

    rmse_eval = RegressionEvaluator(labelCol="trending_days", predictionCol="predicted_days", metricName="rmse")
    mae_eval = RegressionEvaluator(labelCol="trending_days", predictionCol="predicted_days", metricName="mae")
    r2_eval = RegressionEvaluator(labelCol="trending_days", predictionCol="predicted_days", metricName="r2")

    model_rmse = rmse_eval.evaluate(predictions)
    model_mae = mae_eval.evaluate(predictions)
    model_r2 = r2_eval.evaluate(predictions)

    # Baseline: constant prediction = mean(trending_days) computed on train set.
    baseline_df = predictions.withColumn("predicted_days", F.lit(mean_val))
    baseline_rmse = rmse_eval.evaluate(baseline_df)

    logger.info("--- KET QUA DANH GIA THUC TE ---")
    logger.info(f"[BASELINE (Đoán TB = {mean_val:.2f})] RMSE: {baseline_rmse:.4f}")
    logger.info(f"[OUR MODEL] RMSE: {model_rmse:.4f} | MAE: {model_mae:.4f} | R2: {model_r2:.4f}")
    
    if model_rmse < baseline_rmse:
        logger.info("✓ SUCCESS: Mô hình học tốt và vượt mốc Baseline.")
    
    metrics = {"rmse": model_rmse, "mae": model_mae, "r2": model_r2}
    return model, predictions, metrics