from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator

def train_spark_model(df_features, num_trees=100, max_depth=12):
    print("\n" + "=" * 80)
    print("MODEL TRAINING (Regression - Temporal Split & Log Transform)")
    print("=" * 80)
    
    feature_cols = [
        'log_views', 'log_likes', 'log_comment_count', 
        'like_ratio', 'comment_ratio',
        'title_length', 'tag_count', 'publish_hour', 'publish_dow'
    ]
    
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features", handleInvalid="keep")
    
    # Temporal Split (Không dùng randomSplit)
    cutoff_date = "2018-04-01" 
    train = df_features.filter(F.col("parsed_trending_date") < cutoff_date)
    test = df_features.filter(F.col("parsed_trending_date") >= cutoff_date)
    
    print(f"\n[Temporal Split] Training (Before {cutoff_date}): {train.count():,} rows")
    print(f"[Temporal Split] Testing (After {cutoff_date}): {test.count():,} rows")
    
    # Train trên Target đã Log-transform
    rf = RandomForestRegressor(
        featuresCol="features", 
        labelCol="log_trending_days", 
        numTrees=int(num_trees), 
        maxDepth=int(max_depth),
        seed=42
    )
    
    pipeline = Pipeline(stages=[assembler, rf])
    print("\n[Training...]")
    model = pipeline.fit(train)
    
    predictions = model.transform(test)
    
    # Đảo ngược Log (expm1) để lấy giá trị số ngày thật
    predictions = predictions.withColumn("predicted_days", F.expr("expm1(prediction)"))
    
    # Tính Baseline (Mean của tập Train)
    mean_target_row = train.select(F.avg("trending_days").alias("mean_val")).collect()[0]
    baseline_mean = mean_target_row["mean_val"]
    predictions = predictions.withColumn("baseline_pred", F.lit(baseline_mean))
    
    # Đánh giá Model vs Baseline
    rmse_eval = RegressionEvaluator(labelCol="trending_days", predictionCol="predicted_days", metricName="rmse")
    mae_eval = RegressionEvaluator(labelCol="trending_days", predictionCol="predicted_days", metricName="mae")
    r2_eval = RegressionEvaluator(labelCol="trending_days", predictionCol="predicted_days", metricName="r2")
    
    model_rmse = rmse_eval.evaluate(predictions)
    model_mae = mae_eval.evaluate(predictions)
    model_r2 = r2_eval.evaluate(predictions)
    
    baseline_rmse = rmse_eval.evaluate(predictions.withColumnRenamed("baseline_pred", "predicted_days"))
    baseline_mae = mae_eval.evaluate(predictions.withColumnRenamed("baseline_pred", "predicted_days"))
    
    print(f"\nMetrics on Test Set (Reverted to Real Days):")
    print(f"  [BASELINE (Mean = {baseline_mean:.2f})] RMSE: {baseline_rmse:.4f} | MAE: {baseline_mae:.4f}")
    print(f"  [OUR MODEL] RMSE: {model_rmse:.4f} | MAE: {model_mae:.4f} | R2: {model_r2:.4f}")
    
    if model_rmse < baseline_rmse:
        print("  => ✓ SUCCESS: Model outperforms predicting the mean!")
    else:
        print("  => ✗ WARNING: Model is worse than predicting the mean.")
    
    # Feature Importances
    rf_model = model.stages[-1]
    importances = list(zip(feature_cols, rf_model.featureImportances))
    importances.sort(key=lambda x: x[1], reverse=True)
    print("\nTop Feature Importances:")
    for i, (name, imp) in enumerate(importances[:5], 1):
        print(f"  {i}. {name}: {imp:.4f} ({(imp*100):.2f}%)")

    metrics = {"rmse": model_rmse, "mae": model_mae, "r2": model_r2}
    return model, predictions, metrics