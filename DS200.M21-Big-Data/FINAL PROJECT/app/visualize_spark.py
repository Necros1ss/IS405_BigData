"""
Visualization module for Spark ML models.
Generates plots for model performance and feature importance.
"""
import os
import json
from typing import Dict, Any, Tuple
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend


def create_output_dir(output_dir: str = "Images") -> str:
    """Ensure output directory exists."""
    os.makedirs(output_dir, exist_ok=True)
    return output_dir


def plot_feature_importance(metrics: Dict[str, Any], output_dir: str = "Images") -> str:
    """
    Create a bar chart of feature importances.
    
    Args:
        metrics: Dictionary with "feature_importances" key
        output_dir: Directory to save the plot
    
    Returns:
        Path to saved figure
    """
    output_dir = create_output_dir(output_dir)
    
    if "feature_importances" not in metrics:
        print("Warning: No feature_importances in metrics")
        return None
    
    importances = metrics["feature_importances"]
    
    fig, ax = plt.subplots(figsize=(10, 6))
    features = list(importances.keys())
    values = list(importances.values())
    
    colors = plt.cm.viridis([(v - min(values)) / (max(values) - min(values)) for v in values])
    ax.barh(features, values, color=colors)
    ax.set_xlabel("Importance Score", fontsize=12)
    ax.set_ylabel("Features", fontsize=12)
    ax.set_title("RandomForest Feature Importance", fontsize=14, fontweight="bold")
    ax.grid(axis="x", alpha=0.3)
    
    for i, v in enumerate(values):
        ax.text(v, i, f" {v:.4f}", va="center", fontsize=10)
    
    plt.tight_layout()
    output_path = os.path.join(output_dir, "feature_importance.png")
    plt.savefig(output_path, dpi=100, bbox_inches="tight")
    plt.close()
    
    print(f"Saved feature importance plot to {output_path}")
    return output_path


def plot_model_metrics(metrics: Dict[str, Any], output_dir: str = "Images") -> str:
    """
    Create a summary of model metrics.
    
    Args:
        metrics: Dictionary with "auc" and other metrics
        output_dir: Directory to save the plot
    
    Returns:
        Path to saved figure
    """
    output_dir = create_output_dir(output_dir)
    
    fig, ax = plt.subplots(figsize=(8, 6))
    
    # Create simple text-based metrics display
    auc = metrics.get("auc", 0.0)
    
    ax.text(0.5, 0.7, "Model Performance", fontsize=16, fontweight="bold", ha="center")
    ax.text(0.5, 0.5, f"AUC Score: {auc:.4f}", fontsize=14, ha="center", 
            bbox=dict(boxstyle="round", facecolor="lightblue", alpha=0.7))
    ax.text(0.5, 0.3, f"Status: {'✓ Good' if auc > 0.7 else '⚠ Fair' if auc > 0.6 else '✗ Poor'}", 
            fontsize=12, ha="center")
    
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis("off")
    
    output_path = os.path.join(output_dir, "model_metrics.png")
    plt.savefig(output_path, dpi=100, bbox_inches="tight")
    plt.close()
    
    print(f"Saved model metrics plot to {output_path}")
    return output_path


def plot_data_distribution(df, output_dir: str = "Images") -> Tuple[str, str]:
    """
    Create histograms of key features.
    
    Args:
        df: PySpark DataFrame with features
        output_dir: Directory to save plots
    
    Returns:
        Tuple of paths to saved figures (engagement, like_ratio)
    """
    output_dir = create_output_dir(output_dir)
    
    # Convert to Pandas for plotting (sample if too large)
    sample_size = min(df.count(), 10000)
    pandas_df = df.sample(False, min(sample_size / df.count(), 1.0)).toPandas()
    
    # Plot engagement distribution
    fig, ax = plt.subplots(figsize=(10, 6))
    if "engagement" in pandas_df.columns:
        ax.hist(pandas_df["engagement"], bins=50, color="skyblue", edgecolor="black", alpha=0.7)
        ax.set_xlabel("Engagement (Likes + Comments)", fontsize=12)
        ax.set_ylabel("Frequency", fontsize=12)
        ax.set_title("Distribution of Engagement Metric", fontsize=14, fontweight="bold")
        ax.grid(axis="y", alpha=0.3)
        
        engagement_path = os.path.join(output_dir, "data_engagement_dist.png")
        plt.savefig(engagement_path, dpi=100, bbox_inches="tight")
        plt.close()
        print(f"Saved engagement distribution plot to {engagement_path}")
    else:
        engagement_path = None
    
    # Plot like_ratio distribution
    fig, ax = plt.subplots(figsize=(10, 6))
    if "like_ratio" in pandas_df.columns:
        ax.hist(pandas_df["like_ratio"], bins=50, color="lightgreen", edgecolor="black", alpha=0.7)
        ax.set_xlabel("Like Ratio (Likes / Views)", fontsize=12)
        ax.set_ylabel("Frequency", fontsize=12)
        ax.set_title("Distribution of Like Ratio", fontsize=14, fontweight="bold")
        ax.grid(axis="y", alpha=0.3)
        
        ratio_path = os.path.join(output_dir, "data_like_ratio_dist.png")
        plt.savefig(ratio_path, dpi=100, bbox_inches="tight")
        plt.close()
        print(f"Saved like ratio distribution plot to {ratio_path}")
    else:
        ratio_path = None
    
    return engagement_path, ratio_path


def save_metrics_json(metrics: Dict[str, Any], output_path: str = "/tmp/rf_metrics.json") -> str:
    """
    Save metrics to JSON file.
    
    Args:
        metrics: Metrics dictionary
        output_path: Path to save JSON
    
    Returns:
        Path to saved file
    """
    with open(output_path, 'w') as f:
        json.dump(metrics, f, indent=2, default=str)
    print(f"Saved metrics JSON to {output_path}")
    return output_path


def generate_all_visualizations(
    metrics: Dict[str, Any],
    df_features=None,
    output_dir: str = "Images"
) -> Dict[str, str]:
    """
    Generate all visualizations at once.
    
    Args:
        metrics: Model metrics dictionary
        df_features: PySpark DataFrame with features (optional)
        output_dir: Directory to save plots
    
    Returns:
        Dictionary mapping plot names to file paths
    """
    outputs = {}
    
    # Always generate these
    outputs["feature_importance"] = plot_feature_importance(metrics, output_dir)
    outputs["model_metrics"] = plot_model_metrics(metrics, output_dir)
    
    # Generate distribution plots if data provided
    if df_features is not None:
        engagement_plot, ratio_plot = plot_data_distribution(df_features, output_dir)
        outputs["engagement_distribution"] = engagement_plot
        outputs["like_ratio_distribution"] = ratio_plot
    
    print(f"\n✓ Generated {len([v for v in outputs.values() if v])} visualization(s)")
    return outputs
