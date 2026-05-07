import logging
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator

logger = logging.getLogger(__name__)

def train_spark_model(df_features, num_trees=100, max_depth=12):
    logger.info("BẮT ĐẦU HUẤN LUYỆN MÔ HÌNH (Multi-Country + Temporal Split)")
    
    # 1. Chuyển đổi Country dạng String sang dạng Số (Index)
    country_indexer = StringIndexer(inputCol="country", outputCol="country_index", handleInvalid="keep")
    
    # 2. Bổ sung country_index vào danh sách đặc trưng
    feature_cols = [
        'country_index', # Feature mới giải quyết Mixed-Distribution
        'log_views', 'log_likes', 'log_comment_count', 
        'like_ratio', 'comment_ratio',
        'title_length', 'tag_count', 'publish_hour', 'publish_dow',
        'log_hours_to_trending' 
    ]
    
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features", handleInvalid="keep")
    
    # TEMPORAL SPLIT
    cutoff_date = "2018-04-01" 
    train = df_features.filter(F.col("parsed_trending_date") < cutoff_date)
    test = df_features.filter(F.col("parsed_trending_date") >= cutoff_date)
    
    logger.info(f"Tập Huấn Luyện (Trước {cutoff_date}): {train.count():,} videos")
    logger.info(f"Tập Kiểm Thử (Sau {cutoff_date}): {test.count():,} videos")
    
    rf = RandomForestRegressor(
        featuresCol="features", 
        labelCol="log_trending_days", 
        numTrees=int(num_trees), 
        maxDepth=int(max_depth),
        seed=42
    )
    
    # Nắp ráp Pipeline (Thêm country_indexer vào đầu)
    pipeline = Pipeline(stages=[country_indexer, assembler, rf])
    logger.info("Đang Fit Model...")
    model = pipeline.fit(train)
    
    # ĐÁNH GIÁ
    predictions = model.transform(test)
    predictions = predictions.withColumn("predicted_days", F.expr("expm1(prediction)"))
    
    mean_val = train.select(F.avg("trending_days").alias("mean_val")).collect()[0]["mean_val"]

    rmse_eval = RegressionEvaluator(labelCol="trending_days", predictionCol="predicted_days", metricName="rmse")
    mae_eval = RegressionEvaluator(labelCol="trending_days", predictionCol="predicted_days", metricName="mae")
    r2_eval = RegressionEvaluator(labelCol="trending_days", predictionCol="predicted_days", metricName="r2")

    model_rmse = rmse_eval.evaluate(predictions)
    model_mae = mae_eval.evaluate(predictions)
    model_r2 = r2_eval.evaluate(predictions)

    # Baseline: constant prediction = mean(trending_days) computed on train set
    baseline_df = predictions.withColumn("predicted_days", F.lit(mean_val))
    baseline_rmse = rmse_eval.evaluate(baseline_df)
    
    logger.info(f"--- KẾT QUẢ ĐÁNH GIÁ THỰC TẾ ---")
    logger.info(f"[BASELINE (Đoán TB = {mean_val:.2f})] RMSE: {baseline_rmse:.4f}")
    logger.info(f"[OUR MODEL] RMSE: {model_rmse:.4f} | MAE: {model_mae:.4f} | R2: {model_r2:.4f}")
    
    if model_rmse < baseline_rmse:
        logger.info("✓ SUCCESS: Mô hình học tốt và vượt mốc Baseline.")
    
    metrics = {"rmse": model_rmse, "mae": model_mae, "r2": model_r2}
    return model, predictions, metrics