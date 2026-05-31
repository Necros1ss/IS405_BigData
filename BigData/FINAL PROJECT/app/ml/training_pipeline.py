#!/usr/bin/env python3
"""Train and compare Spark regression models for trending-day prediction."""

import json
import os
from typing import List, Tuple

import numpy as np
import pandas as pd

from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.regression import GBTRegressor, LinearRegression, RandomForestRegressor
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.sql import functions as F

from app.utils.feature_engineering import add_shared_features

try:
    from xgboost import XGBRegressor
except Exception:
    XGBRegressor = None

try:
    from lightgbm import LGBMRegressor
except Exception:
    LGBMRegressor = None


TARGET_COL = "log_trending_days"
LABEL_COL = "trending_days"


def get_feature_columns():
    numeric_features = [
        "views_normalized",
        "log_views",
        "log_likes",
        "log_comment_count",
        "engagement_rate",
        "like_ratio",
        "comment_ratio",
        "views_per_hour",
        "likes_per_hour",
        "comments_per_hour",
        "title_length",
        "description_length",
        "tag_count",
        "publish_hour",
        "publish_day_of_week",
        "log_hours_to_trending",
    ]
    categorical_features = ["country", "categoryId"]
    return numeric_features, categorical_features


def _build_preprocessing_stages(categorical_cols):
    indexers = [
        StringIndexer(inputCol=col_name, outputCol=f"{col_name}_idx", handleInvalid="keep")
        for col_name in categorical_cols
    ]
    encoders = [
        OneHotEncoder(inputCol=f"{col_name}_idx", outputCol=f"{col_name}_vec", handleInvalid="keep")
        for col_name in categorical_cols
    ]
    return indexers, encoders


def _build_pipeline(numeric_features, categorical_features, estimator):
    indexers, encoders = _build_preprocessing_stages(categorical_features)
    assembler = VectorAssembler(
        inputCols=numeric_features + [f"{column}_vec" for column in categorical_features],
        outputCol="features",
        handleInvalid="keep",
    )
    return Pipeline(stages=[*indexers, *encoders, assembler, estimator])


def _evaluate_predictions(predictions):
    rmse_eval = RegressionEvaluator(labelCol=LABEL_COL, predictionCol="prediction", metricName="rmse")
    mae_eval = RegressionEvaluator(labelCol=LABEL_COL, predictionCol="prediction", metricName="mae")
    r2_eval = RegressionEvaluator(labelCol=LABEL_COL, predictionCol="prediction", metricName="r2")
    return {
        "rmse": float(rmse_eval.evaluate(predictions)),
        "mae": float(mae_eval.evaluate(predictions)),
        "r2": float(r2_eval.evaluate(predictions)),
    }


def _prepare_predictions(model, test_df):
    transformed = model.transform(test_df)
    transformed = transformed.withColumn("prediction", F.expr("expm1(raw_prediction)"))
    transformed = transformed.withColumn("predicted_trending_days", F.col("prediction"))
    return transformed


def _feature_importance_from_model(model, feature_names):
    estimator_model = model.stages[-1]
    if hasattr(estimator_model, "featureImportances"):
        importances = estimator_model.featureImportances.toArray().tolist()
        return [
            {"feature": feature_name, "importance": float(value)}
            for feature_name, value in sorted(zip(feature_names, importances), key=lambda item: item[1], reverse=True)
        ]

    if hasattr(estimator_model, "coefficients"):
        coefficients = [abs(float(value)) for value in estimator_model.coefficients.toArray().tolist()]
        return [
            {"feature": feature_name, "importance": float(value)}
            for feature_name, value in sorted(zip(feature_names, coefficients), key=lambda item: item[1], reverse=True)
        ]

    return []


def _to_optional_model_frame(df, numeric_features, categorical_features, sample_limit=50000, seed=42):
    selected_cols = [*numeric_features, *categorical_features, LABEL_COL]
    optional_df = df.select(*selected_cols)
    if optional_df.count() > sample_limit:
        optional_df = optional_df.orderBy(F.rand(seed)).limit(sample_limit)
    pdf = optional_df.toPandas()

    for column in numeric_features + [LABEL_COL]:
        pdf[column] = pd.to_numeric(pdf[column], errors="coerce").fillna(0.0)

    for column in categorical_features:
        pdf[column] = pdf[column].fillna("UNKNOWN").astype(str)

    return pdf


def _align_optional_frames(train_pdf, test_pdf, categorical_features):
    train_encoded = pd.get_dummies(train_pdf, columns=categorical_features, dummy_na=False)
    test_encoded = pd.get_dummies(test_pdf, columns=categorical_features, dummy_na=False)
    train_encoded, test_encoded = train_encoded.align(test_encoded, join="left", axis=1, fill_value=0)

    y_train = np.log1p(train_encoded.pop(LABEL_COL).astype(float))
    y_test = test_encoded.pop(LABEL_COL).astype(float)
    return train_encoded, test_encoded, y_train, y_test


def _train_optional_model(model_name, estimator_factory, train_pdf, test_pdf, categorical_features):
    if estimator_factory is None:
        return None

    train_encoded, test_encoded, y_train, y_test = _align_optional_frames(train_pdf, test_pdf, categorical_features)
    estimator = estimator_factory()
    estimator.fit(train_encoded, y_train)
    log_predictions = estimator.predict(test_encoded)
    predictions = np.expm1(log_predictions)

    rmse = float(np.sqrt(np.mean((y_test - predictions) ** 2)))
    mae = float(np.mean(np.abs(y_test - predictions)))
    r2 = float(1.0 - np.sum((y_test - predictions) ** 2) / max(np.sum((y_test - np.mean(y_test)) ** 2), 1e-9))

    feature_importance = []
    if hasattr(estimator, "feature_importances_"):
        importances = np.asarray(estimator.feature_importances_, dtype=float)
        feature_importance = [
            {"feature": feature_name, "importance": float(value)}
            for feature_name, value in sorted(zip(train_encoded.columns, importances), key=lambda item: item[1], reverse=True)
        ]

    metrics = {
        "model_name": model_name,
        "rmse": rmse,
        "mae": mae,
        "r2": r2,
        "feature_importance": feature_importance,
        "supported": True,
    }
    return metrics


def train_spark_model(df, num_trees=100, max_depth=12, tune=True):
    df = add_shared_features(df, snapshot_ts_col="trending_date_parsed")
    cutoff_date = "2026-01-01"
    train_df = df.filter(F.col("trending_date_parsed") < cutoff_date)
    test_df = df.filter(F.col("trending_date_parsed") >= cutoff_date)

    train_count = train_df.count()
    test_count = test_df.count()

    if test_count == 0:
        raise ValueError("Test dataset is empty. Choose another cutoff date.")

    numeric_features, categorical_features = get_feature_columns()
    feature_names = numeric_features + [f"{column}_vec" for column in categorical_features]

    candidate_results = []
    optional_results = []

    def fit_and_score(model_name, estimator, use_cv=False, param_grid=None):
        pipeline = _build_pipeline(numeric_features, categorical_features, estimator)
        if use_cv:
            cross_validator = CrossValidator(
                estimator=pipeline,
                estimatorParamMaps=param_grid,
                evaluator=RegressionEvaluator(labelCol=TARGET_COL, predictionCol="raw_prediction", metricName="rmse"),
                numFolds=3,
                parallelism=2,
                seed=42,
            )
            cv_model = cross_validator.fit(train_df)
            model = cv_model.bestModel
        else:
            model = pipeline.fit(train_df)

        predictions = _prepare_predictions(model, test_df)
        scores = {
            **_evaluate_predictions(predictions),
            "model_name": model_name,
        }
        candidate_results.append((model_name, model, predictions, scores))
        return model, predictions, scores

    lr_estimator = LinearRegression(
        featuresCol="features",
        labelCol=TARGET_COL,
        predictionCol="raw_prediction",
        maxIter=80,
        regParam=0.05,
    )
    fit_and_score("linear_regression", lr_estimator, use_cv=False)

    gbt_estimator = GBTRegressor(
        featuresCol="features",
        labelCol=TARGET_COL,
        predictionCol="raw_prediction",
        seed=42,
        maxIter=60,
        maxDepth=6,
    )
    fit_and_score("gradient_boosting", gbt_estimator, use_cv=False)

    rf_estimator = RandomForestRegressor(
        featuresCol="features",
        labelCol=TARGET_COL,
        predictionCol="raw_prediction",
        seed=42,
        numTrees=num_trees,
        maxDepth=max_depth,
    )

    if tune:
        rf_param_grid = (
            ParamGridBuilder()
            .addGrid(rf_estimator.numTrees, sorted({max(20, num_trees - 20), num_trees, num_trees + 20}))
            .addGrid(rf_estimator.maxDepth, sorted({max(4, max_depth - 2), max_depth, max_depth + 2}))
            .addGrid(rf_estimator.minInstancesPerNode, [1, 2])
            .addGrid(rf_estimator.maxBins, [32, 64])
            .build()
        )
        fit_and_score("random_forest_tuned", rf_estimator, use_cv=True, param_grid=rf_param_grid)
    else:
        fit_and_score("random_forest", rf_estimator, use_cv=False)

    baseline_mean = train_df.select(F.avg(LABEL_COL).alias("baseline_mean")).first()["baseline_mean"] or 0.0
    baseline_predictions = test_df.withColumn("prediction", F.lit(float(baseline_mean)))
    baseline_scores = {
        **_evaluate_predictions(baseline_predictions),
        "model_name": "mean_baseline",
    }
    candidate_results.append(("mean_baseline", None, baseline_predictions, baseline_scores))

    train_pdf = _to_optional_model_frame(train_df, numeric_features, categorical_features)
    test_pdf = _to_optional_model_frame(test_df, numeric_features, categorical_features, sample_limit=15000)

    if XGBRegressor is not None:
        optional_xgb = _train_optional_model(
            "xgboost",
            lambda: XGBRegressor(
                n_estimators=300,
                max_depth=6,
                learning_rate=0.05,
                subsample=0.9,
                colsample_bytree=0.9,
                objective="reg:squarederror",
                random_state=42,
            ),
            train_pdf,
            test_pdf,
            categorical_features,
        )
        if optional_xgb is not None:
            optional_results.append(optional_xgb)

    if LGBMRegressor is not None:
        optional_lgbm = _train_optional_model(
            "lightgbm",
            lambda: LGBMRegressor(
                n_estimators=300,
                learning_rate=0.05,
                max_depth=-1,
                subsample=0.9,
                colsample_bytree=0.9,
                random_state=42,
            ),
            train_pdf,
            test_pdf,
            categorical_features,
        )
        if optional_lgbm is not None:
            optional_results.append(optional_lgbm)

    best_name, best_model, best_predictions, best_scores = min(
        (result for result in candidate_results if result[1] is not None),
        key=lambda item: item[3]["rmse"],
    )

    best_overall_optional = min(optional_results, key=lambda item: item["rmse"], default=None)

    comparison_rows = [
        {
            "model_name": scores["model_name"],
            "rmse": scores["rmse"],
            "mae": scores["mae"],
            "r2": scores["r2"],
            "model_family": "spark",
        }
        for _, _, _, scores in candidate_results
    ]

    comparison_rows.extend(
        {
            "model_name": result["model_name"],
            "rmse": result["rmse"],
            "mae": result["mae"],
            "r2": result["r2"],
            "model_family": "optional",
        }
        for result in optional_results
    )

    metrics = {
        "best_model": best_name,
        "best_overall_model": best_overall_optional["model_name"] if best_overall_optional else best_name,
        "rmse": float(best_scores["rmse"]),
        "mae": float(best_scores["mae"]),
        "r2": float(best_scores["r2"]),
        "baseline_rmse": float(baseline_scores["rmse"]),
        "baseline_mae": float(baseline_scores["mae"]),
        "baseline_r2": float(baseline_scores["r2"]),
        "train_rows": int(train_count),
        "test_rows": int(test_count),
        "feature_columns": numeric_features,
        "categorical_columns": categorical_features,
        "model_comparison": comparison_rows,
        "feature_importance": _feature_importance_from_model(best_model, feature_names),
        "optional_model_comparison": optional_results,
    }

    os.makedirs("metrics", exist_ok=True)
    with open("metrics/regression_metrics.json", "w", encoding="utf-8") as handle:
        json.dump(metrics, handle, indent=2)

    return best_model, best_predictions, metrics
