#!/usr/bin/env python3
"""Aggregate per-model training metrics into a single comparison report."""

import argparse
import json
import os
import shutil


def _load_json(path):
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def _copy_model_dir(source_dir, target_dir):
    if os.path.exists(target_dir):
        shutil.rmtree(target_dir)
    shutil.copytree(source_dir, target_dir)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--model-output-dir", default="models")
    parser.add_argument("--metrics-output", default="metrics/regression_metrics.json")
    parser.add_argument("--best-model-link", default="models/rf_regression_model")
    args = parser.parse_args()

    model_names = ["linear_regression", "gradient_boosting", "random_forest", "random_forest_tuned", "mean_baseline"]
    comparison_rows = []
    loaded_metrics = []

    for model_name in model_names:
        metrics_path = os.path.join(args.model_output_dir, f"{model_name}_metrics.json")
        if not os.path.exists(metrics_path):
            continue
        metrics = _load_json(metrics_path)
        loaded_metrics.append(metrics)
        comparison_rows.append({
            "model_name": metrics["model_name"],
            "rmse": metrics["rmse"],
            "mae": metrics["mae"],
            "r2": metrics["r2"],
            "model_family": "spark",
        })

    if not loaded_metrics:
        raise RuntimeError("No per-model metrics found to aggregate.")

    best_metrics = min((item for item in loaded_metrics if item["model_name"] != "mean_baseline"), key=lambda item: item["rmse"], default=None)
    if best_metrics is None:
        best_metrics = min(loaded_metrics, key=lambda item: item["rmse"])

    best_model_dir = os.path.join(args.model_output_dir, best_metrics["model_name"])
    if os.path.exists(best_model_dir):
        _copy_model_dir(best_model_dir, args.best_model_link)

    aggregated = {
        "best_model": best_metrics["model_name"],
        "best_overall_model": best_metrics["model_name"],
        "rmse": float(best_metrics["rmse"]),
        "mae": float(best_metrics["mae"]),
        "r2": float(best_metrics["r2"]),
        "baseline_rmse": next((float(item["rmse"]) for item in loaded_metrics if item["model_name"] == "mean_baseline"), None),
        "baseline_mae": next((float(item["mae"]) for item in loaded_metrics if item["model_name"] == "mean_baseline"), None),
        "baseline_r2": next((float(item["r2"]) for item in loaded_metrics if item["model_name"] == "mean_baseline"), None),
        "model_comparison": comparison_rows,
        "aggregated_models": [item["model_name"] for item in loaded_metrics],
        "best_model_dir": best_model_dir,
    }

    os.makedirs(os.path.dirname(args.metrics_output), exist_ok=True)
    with open(args.metrics_output, "w", encoding="utf-8") as handle:
        json.dump(aggregated, handle, indent=2)

    print(json.dumps({"best_model": best_metrics["model_name"], "best_model_dir": best_model_dir}, indent=2))


if __name__ == "__main__":
    main()