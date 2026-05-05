"""
Training utilities for Spark ML (RandomForest pipeline).
"""
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator


def train_spark_model(df_features, sample_fraction=0.01, no_sample=False, num_trees=10, max_depth=6):
    # Create label: trending if engagement > median
    median_val = df_features.approxQuantile("engagement", [0.5], 0.01)[0]
    print(f"Median engagement (approx): {median_val}")
    df_labeled = df_features.withColumn("label", F.when(F.col("engagement") > F.lit(median_val), 1).otherwise(0))

    # Keep engagement out of the feature vector to reduce label leakage.
    feature_cols = ['tag_count', 'description_length', 'like_ratio', 'comment_ratio']
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

    # Split and train
    train, test = df_labeled.randomSplit([0.8, 0.2], seed=42)

    if not no_sample and sample_fraction is not None and 0 < sample_fraction < 1.0:
        train = train.sample(False, float(sample_fraction), seed=42)
        test = test.sample(False, float(sample_fraction), seed=42)
        print(f"Training on {train.count()} sampled rows, testing on {test.count()} sampled rows (fraction={sample_fraction})")
    else:
        print(f"Training on {train.count()} rows, testing on {test.count()} rows (no sampling)")

    rf = RandomForestClassifier(featuresCol="features", labelCol="label", numTrees=int(num_trees), maxDepth=int(max_depth))
    pipeline = Pipeline(stages=[assembler, rf])

    model = pipeline.fit(train)

    # Evaluate
    predictions = model.transform(test)
    evaluator = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC")
    auc = evaluator.evaluate(predictions)
    print(f"Model AUC on test set: {auc:.4f}")

    # Feature importances from RF stage
    rf_model = model.stages[-1]
    metrics = {"auc": float(auc)}
    try:
        importances = rf_model.featureImportances
        print("Feature importances:")
        imp_dict = {}
        for name, imp in zip(feature_cols, importances):
            print(f"  {name}: {imp:.4f}")
            imp_dict[name] = float(imp)
        metrics["feature_importances"] = imp_dict
    except Exception:
        pass

    return model, predictions, metrics
