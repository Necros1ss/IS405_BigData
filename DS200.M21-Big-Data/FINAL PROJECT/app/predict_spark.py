"""
Prediction utilities for Spark ML pipeline.
"""
from pyspark.sql import SparkSession


def predict_sample(model):
    """Generate and show sample predictions using the trained PipelineModel."""
    from pyspark.sql import Row

    samples = [Row(tag_count=40, description_length=800, like_ratio=0.10, comment_ratio=0.05),
               Row(tag_count=5, description_length=100, like_ratio=0.01, comment_ratio=0.001),
               Row(tag_count=25, description_length=400, like_ratio=0.08, comment_ratio=0.03)]
    spark_sess = SparkSession.builder.getOrCreate()
    df_new = spark_sess.createDataFrame(samples)
    preds = model.transform(df_new)
    preds.select("tag_count", "description_length", "like_ratio", "comment_ratio", "prediction", "probability").show(truncate=False)
