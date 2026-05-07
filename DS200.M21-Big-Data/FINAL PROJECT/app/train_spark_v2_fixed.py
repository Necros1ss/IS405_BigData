"""
Training utilities for Spark ML Regression.
Predicts the NUMBER OF DAYS a video will remain trending based on Day-1 features.
"""
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler, VectorIndexer
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator

def prepare_data_regression(df):
    """
    Validation and info printing for prepared regression data.
    """
    print("\n" + "=" * 80)
    print("DATA VALIDATION (Regression)")
    print("=" * 80)
    
    print(f"\nDataset size: {df.count()} rows")
    
    # Show Target (trending_days) statistics
    stats = df.select(
        F.min("trending_days").alias("min_days"),
        F.max("trending_days").alias("max_days"),
        F.avg("trending_days").alias("avg_days")
    ).collect()[0]
    
    print(f"\nTarget (trending_days) Statistics:")
    print(f"  Min: {stats['min_days']} days")
    print(f"  Max: {stats['max_days']} days")
    print(f"  Avg: {stats['avg_days']:.2f} days")
    
    return df

def train_spark_model_regression(df_features, sample_fraction=0.01, no_sample=False, num_trees=100, max_depth=12):
    """
    Train RandomForestRegressor with Day-1 features only (no data leakage).
    """
    print("\n" + "=" * 80)
    print("MODEL TRAINING (Regression)")
    print("=" * 80)
    
    # Features từ Day-1 snapshot
    feature_cols = [
        'views', 'likes', 'dislikes', 'comment_count',
        'title_length', 'tag_count', 'publish_hour', 'publish_dow',
        'comments_disabled', 'ratings_disabled', 'video_error_or_removed'
    ]
    
    print(f"\nFeatures used (Day-1 snapshot):")
    for i, col in enumerate(feature_cols, 1):
        print(f"  {i}. {col}")
    
    # Lắp ráp đặc trưng
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="raw_features")
    indexer = VectorIndexer(
        inputCol="raw_features", 
        outputCol="features", 
        maxCategories=24, 
        handleInvalid="keep"
    )
    
    # Split data (Random split được sử dụng vì dữ liệu đầu vào đã được làm sạch để chỉ giữ lại Day-1)
    train, test = df_features.randomSplit([0.8, 0.2], seed=42)
    
    # Apply sampling if requested
    if not no_sample and sample_fraction is not None and 0 < sample_fraction < 1.0:
        train = train.sample(False, float(sample_fraction), seed=42)
        test = test.sample(False, float(sample_fraction), seed=42)
        print(f"\nTraining on {train.count()} sampled rows")
        print(f"Testing on {test.count()} sampled rows (fraction={sample_fraction})")
    else:
        train_count = train.count()
        test_count = test.count()
        print(f"\nTraining on {train_count:,} rows")
        print(f"Testing on {test_count:,} rows")
    
    # Train Random Forest Regressor
    print(f"\nRandom Forest Regressor Parameters:")
    print(f"  Num Trees: {num_trees}")
    print(f"  Max Depth: {max_depth}")
    print(f"  Seed: 42")
    
    rf = RandomForestRegressor(
        featuresCol="features", 
        labelCol="trending_days", 
        numTrees=int(num_trees), 
        maxDepth=int(max_depth),
        seed=42
    )
    
    # Build pipeline
    pipeline = Pipeline(stages=[assembler, indexer, rf])
    print("\n[Training...]")
    model = pipeline.fit(train)
    
    # Evaluate on test set
    print("\n" + "-" * 80)
    print("MODEL EVALUATION")
    print("-" * 80)
    
    predictions = model.transform(test)
    
    # Regression Evaluators
    rmse_eval = RegressionEvaluator(labelCol="trending_days", predictionCol="prediction", metricName="rmse")
    mae_eval = RegressionEvaluator(labelCol="trending_days", predictionCol="prediction", metricName="mae")
    r2_eval = RegressionEvaluator(labelCol="trending_days", predictionCol="prediction", metricName="r2")
    
    rmse = rmse_eval.evaluate(predictions)
    mae = mae_eval.evaluate(predictions)
    r2 = r2_eval.evaluate(predictions)
    
    print(f"\nMetrics on Test Set:")
    print(f"  RMSE (Root Mean Squared Error): {rmse:.4f} days")
    print(f"  MAE (Mean Absolute Error): {mae:.4f} days")
    print(f"  R² Score: {r2:.4f}")
    
    # Feature importances
    print(f"\n" + "-" * 80)
    print("FEATURE IMPORTANCES")
    print("-" * 80)
    
    rf_model = model.stages[-1]
    metrics = {
        "rmse": float(rmse), 
        "mae": float(mae), 
        "r2": float(r2)
    }
    
    try:
        importances = rf_model.featureImportances
        print("\nFeature Importance Ranking:")
        importance_list = list(zip(feature_cols, importances))
        importance_list.sort(key=lambda x: x[1], reverse=True)
        
        imp_dict = {}
        for i, (name, imp) in enumerate(importance_list, 1):
            pct = imp * 100
            print(f"  {i}. {name}: {imp:.4f} ({pct:.2f}%)")
            imp_dict[name] = float(imp)
        
        metrics["feature_importances"] = imp_dict
    except Exception as e:
        print(f"  Error calculating importances: {e}")
    
    # Sample predictions
    print(f"\n" + "-" * 80)
    print("SAMPLE PREDICTIONS (first 10 videos)")
    print("-" * 80)
    
    sample_preds = predictions.select(
        "video_id", 
        "trending_days", 
        F.round("prediction", 2).alias("predicted_days")
    ).limit(10).collect()
    
    for i, row in enumerate(sample_preds, 1):
        err = abs(row['trending_days'] - row['predicted_days'])
        print(f"  {i}. Video {row['video_id']}: Actual = {row['trending_days']} days | Predicted = {row['predicted_days']} days | Error = {err:.2f} days")
        
    print("\n" + "=" * 80)
    return model, predictions, metrics