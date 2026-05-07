"""
Training utilities for Spark ML with REAL features (no data leakage).
Fixed version: Removes like_ratio, comment_ratio and uses only metadata.
"""
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder, VectorIndexer
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml import Pipeline
from pyspark.ml.functions import vector_to_array
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from datetime import datetime


def prepare_data_v2_no_leakage(df):
    """
    Additional validation and info printing for prepared data.
    Most feature engineering is done in clean_spark_v2_fixed.engineer_features_v2_fixed()
    """
    
    print("\n" + "=" * 80)
    print("DATA VALIDATION (v2 - Fixed)")
    print("=" * 80)
    
    # Print statistics
    print(f"\nDataset size: {df.count()} rows")
    print(f"\nTarget distribution:")
    label_dist = df.groupBy("label").count().collect()
    for row in label_dist:
        pct = (row['count'] / df.count()) * 100
        status = "Trending (top 50)" if row['label'] == 1 else "Not Trending"
        print(f"  {status}: {row['count']} ({pct:.2f}%)")
    
    # Show feature statistics
    print(f"\nFeature Statistics:")
    stats = df.select(
        F.min(F.col("title_length")).alias("title_len_min"),
        F.max(F.col("title_length")).alias("title_len_max"),
        F.avg(F.col("title_length")).alias("title_len_avg"),
        F.min(F.col("description_length")).alias("desc_len_min"),
        F.max(F.col("description_length")).alias("desc_len_max"),
        F.avg(F.col("description_length")).alias("desc_len_avg")
    ).collect()[0]
    print(f"  Title Length: min={stats['title_len_min']}, max={stats['title_len_max']}, avg={stats['title_len_avg']:.0f}")
    print(f"  Description Length: min={stats['desc_len_min']}, max={stats['desc_len_max']}, avg={stats['desc_len_avg']:.0f}")
    
    print(f"  Publish Hour: distributed across 0-23")
    print(f"  Publish Day: distributed across 1-7 (1=Sunday)")
    
    return df


def train_spark_model_v2(df_features, sample_fraction=0.01, no_sample=False, num_trees=100, max_depth=12):
    """
    Train model with REAL features only (no data leakage).
    """
    
    print("\n" + "=" * 80)
    print("MODEL TRAINING (v2 - No Data Leakage)")
    print("=" * 80)
    
    # Features: Only metadata available at publish time
    # REMOVED: like_ratio, comment_ratio (LEAKY!)
    # ADDED: title_length, publish_hour, publish_day_of_week, is_english
    feature_cols = ['title_length', 'description_length', 'tag_count', 
                    'publish_hour', 'publish_day_of_week', 'is_english']
    
    print(f"\nFeatures used (no leakage):")
    for i, col in enumerate(feature_cols, 1):
        print(f"  {i}. {col}")
    
    # Assemble features
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="raw_features")

    indexer = VectorIndexer(
        inputCol="raw_features",
        outputCol="features",
        maxCategories=24,
        handleInvalid="keep"
    )
    
    # Split data
    train, test = df_features.randomSplit([0.8, 0.2], seed=42)
    
    # Apply sampling if requested (for debugging with smaller data)
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
    
    # Show training data label distribution
    print(f"\nTraining set label distribution:")
    train_dist = train.groupBy("label").count().collect()
    for row in train_dist:
        pct = (row['count'] / train.count()) * 100
        print(f"  Label {int(row['label'])}: {row['count']} ({pct:.2f}%)")
    
    # Train Random Forest
    print(f"\nRandom Forest Parameters:")
    print(f"  Num Trees: {num_trees}")
    print(f"  Max Depth: {max_depth}")
    print(f"  Impurity: Gini")
    print(f"  Seed: 42")
    
    rf = RandomForestClassifier(
        featuresCol="features", 
        labelCol="label", 
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
    
    # Binary Classification Evaluator (AUC)
    bc_evaluator = BinaryClassificationEvaluator(
        labelCol="label", 
        rawPredictionCol="rawPrediction", 
        metricName="areaUnderROC"
    )
    auc = bc_evaluator.evaluate(predictions)
    
    # Multiclass Evaluator for Accuracy, F1, Precision, Recall
    mc_evaluator = MulticlassClassificationEvaluator(
        labelCol="label",
        predictionCol="prediction",
        metricName="accuracy"
    )
    accuracy = mc_evaluator.evaluate(predictions)
    
    f1_evaluator = MulticlassClassificationEvaluator(
        labelCol="label",
        predictionCol="prediction",
        metricName="f1"
    )
    f1 = f1_evaluator.evaluate(predictions)
    
    print(f"\nMetrics on Test Set ({test.count():,} videos):")
    print(f"  AUC (Area Under ROC): {auc:.4f}")
    print(f"  Accuracy: {accuracy:.4f}")
    print(f"  F1-Score (Weighted): {f1:.4f}")
    
    # Confusion Matrix
    tp = predictions.filter((F.col("label") == 1) & (F.col("prediction") == 1.0)).count()
    tn = predictions.filter((F.col("label") == 0) & (F.col("prediction") == 0.0)).count()
    fp = predictions.filter((F.col("label") == 0) & (F.col("prediction") == 1.0)).count()
    fn = predictions.filter((F.col("label") == 1) & (F.col("prediction") == 0.0)).count()
    
    print(f"\nConfusion Matrix:")
    print(f"  True Positives (trending predicted correctly): {tp}")
    print(f"  True Negatives (non-trending predicted correctly): {tn}")
    print(f"  False Positives (wrongly predicted trending): {fp}")
    print(f"  False Negatives (missed trending): {fn}")
    
    if tp + fn > 0:
        recall = tp / (tp + fn)
        print(f"  Recall (catch trending videos): {recall:.4f}")
    if tp + fp > 0:
        precision = tp / (tp + fp)
        print(f"  Precision (correctly identified trending): {precision:.4f}")
    
    # Feature importances
    print(f"\n" + "-" * 80)
    print("FEATURE IMPORTANCES")
    print("-" * 80)
    
    rf_model = model.stages[-1]
    metrics = {
        "auc": float(auc),
        "accuracy": float(accuracy),
        "f1": float(f1),
        "tp": int(tp),
        "tn": int(tn),
        "fp": int(fp),
        "fn": int(fn)
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
        F.col("title_length"),
        F.col("description_length"),
        F.col("tag_count"),
        F.col("publish_hour"),
        F.col("publish_day_of_week"),
        F.col("is_english"),
        F.col("label"),
        F.col("prediction"),
        F.round(vector_to_array(F.col("probability"))[1], 4).alias("trending_prob")
    ).limit(10).collect()
    
    for i, row in enumerate(sample_preds, 1):
        actual = "Trending" if row[6] == 1 else "Not Trending"
        predicted = "Trending" if row[7] == 1 else "Not Trending"
        prob = row[8]
        match = "✓" if (row[6] == row[7]) else "✗"
        print(
            f"  {i}. {match} features="
            f"(title_len={int(row[0])}, desc_len={int(row[1])}, tags={int(row[2])}, "
            f"hour={int(row[3])}, dow={int(row[4])}, english={int(row[5])})"
        )
        print(f"     Actual: {actual}, Predicted: {predicted} (prob={prob:.2%})")
    
    print("\n" + "=" * 80)
    
    return model, predictions, metrics
