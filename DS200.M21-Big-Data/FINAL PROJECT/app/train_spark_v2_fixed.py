import logging
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator

logger = logging.getLogger(__name__)

def train_spark_model(df_features, num_trees=100, max_depth=12):
    logger.info("BẮT ĐẦU HUẤN LUYỆN MÔ HÌNH (Temporal Split + Log Transform)")
    
    feature_cols = [
        'log_views', 'log_likes', 'log_comment_count', 
        'like_ratio', 'comment_ratio',
        'title_length', 'tag_count', 'publish_hour', 'publish_dow'
    ]
    
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features", handleInvalid="keep")
    
    # CHIA TẬP DỮ LIỆU THEO THỜI GIAN THỰC TẾ (Không dùng randomSplit)
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
    
    pipeline = Pipeline(stages=[assembler, rf])
    logger.info("Đang Fit Model...")
    model = pipeline.fit(train)
    
    # ĐÁNH GIÁ MÔ HÌNH
    predictions = model.transform(test)
    
    # Đảo ngược hàm log (expm1) để có số ngày thực tế
    predictions = predictions.withColumn("predicted_days", F.expr("expm1(prediction)"))
    
    # Tính Baseline (Trung bình của tập Train)
    mean_val = train.select(F.avg("trending_days").alias("mean_val")).collect()[0]["mean_val"]
    predictions = predictions.withColumn("baseline_pred", F.lit(mean_val))
    
    rmse_eval = RegressionEvaluator(labelCol="trending_days", predictionCol="predicted_days", metricName="rmse")
    mae_eval = RegressionEvaluator(labelCol="trending_days", predictionCol="predicted_days", metricName="mae")
    r2_eval = RegressionEvaluator(labelCol="trending_days", predictionCol="predicted_days", metricName="r2")
    
    model_rmse = rmse_eval.evaluate(predictions)
    model_mae = mae_eval.evaluate(predictions)
    model_r2 = r2_eval.evaluate(predictions)
    
    baseline_rmse = rmse_eval.evaluate(predictions.withColumnRenamed("baseline_pred", "predicted_days"))
    
    logger.info(f"--- KẾT QUẢ ĐÁNH GIÁ THỰC TẾ ---")
    logger.info(f"[BASELINE (Dự đoán trung bình = {mean_val:.2f} ngày)] RMSE: {baseline_rmse:.4f}")
    logger.info(f"[MÔ HÌNH CỦA TA] RMSE: {model_rmse:.4f} | MAE: {model_mae:.4f} | R2: {model_r2:.4f}")
    
    if model_rmse < baseline_rmse:
        logger.info("✓ MÔ HÌNH TỐT: Học được quy luật (Vượt qua Baseline).")
    else:
        logger.warning("✗ CẢNH BÁO: Mô hình tệ hơn việc đoán bừa giá trị trung bình.")
    
    metrics = {"rmse": model_rmse, "mae": model_mae, "r2": model_r2}
    return model, predictions, metrics