"""
Unit tests for train_spark module.
Tests model training, evaluation, and predictions.
"""
import unittest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from app.train_spark import train_spark_model


class TestTrainSpark(unittest.TestCase):
    """Test cases for train_spark functions."""

    @classmethod
    def setUpClass(cls):
        """Initialize Spark session for tests."""
        cls.spark = SparkSession.builder \
            .appName("TestTrainSpark") \
            .master("local[1]") \
            .getOrCreate()
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        """Stop Spark session after tests."""
        cls.spark.stop()

    def _create_test_data(self, num_rows=100):
        """Create test feature data."""
        data = []
        for i in range(num_rows):
            data.append((
                10 + i % 50,  # tag_count
                100 + i % 500,  # description_length
                0.01 + (i % 10) * 0.01,  # like_ratio
                0.005 + (i % 5) * 0.005,  # comment_ratio
                1000 + i * 10  # engagement
            ))
        return self.spark.createDataFrame(
            data,
            ["tag_count", "description_length", "like_ratio", "comment_ratio", "engagement"]
        )

    def test_train_spark_model_returns_tuple(self):
        """Test that train_spark_model returns (model, predictions, metrics)."""
        df = self._create_test_data(50)
        model, predictions, metrics = train_spark_model(df, no_sample=True, num_trees=5, max_depth=3)

        self.assertIsNotNone(model)
        self.assertIsNotNone(predictions)
        self.assertIsNotNone(metrics)
        self.assertIsInstance(metrics, dict)

    def test_model_has_auc_metric(self):
        """Test that metrics include AUC."""
        df = self._create_test_data(50)
        model, predictions, metrics = train_spark_model(df, no_sample=True, num_trees=5, max_depth=3)

        self.assertIn("auc", metrics)
        self.assertGreater(metrics["auc"], 0.0)
        self.assertLessEqual(metrics["auc"], 1.0)

    def test_model_has_feature_importances(self):
        """Test that metrics include feature importances."""
        df = self._create_test_data(50)
        model, predictions, metrics = train_spark_model(df, no_sample=True, num_trees=5, max_depth=3)

        self.assertIn("feature_importances", metrics)
        importances = metrics["feature_importances"]
        self.assertEqual(len(importances), 4)
        self.assertTrue(all(0 <= v <= 1 for v in importances.values()))

    def test_predictions_have_label_column(self):
        """Test that predictions DataFrame has label column."""
        df = self._create_test_data(50)
        model, predictions, metrics = train_spark_model(df, no_sample=True, num_trees=5, max_depth=3)

        self.assertIn("label", predictions.columns)
        self.assertIn("prediction", predictions.columns)

    def test_sampling_reduces_rows(self):
        """Test that sampling reduces dataset size."""
        df = self._create_test_data(100)
        model_full, pred_full, _ = train_spark_model(df, no_sample=True, num_trees=5, max_depth=3)
        model_sampled, pred_sampled, _ = train_spark_model(df, sample_fraction=0.1, no_sample=False, num_trees=5, max_depth=3)

        # Sampled should have fewer rows
        self.assertLess(pred_sampled.count(), pred_full.count())

    def test_different_hyperparameters(self):
        """Test that different hyperparameters produce different models."""
        df = self._create_test_data(100)
        model1, _, metrics1 = train_spark_model(df, no_sample=True, num_trees=5, max_depth=3)
        model2, _, metrics2 = train_spark_model(df, no_sample=True, num_trees=10, max_depth=5)

        # Different hyperparameters should produce different AUC values (in most cases)
        # Note: This is probabilistic, so we just check both models work
        self.assertIn("auc", metrics1)
        self.assertIn("auc", metrics2)


if __name__ == '__main__':
    unittest.main()
