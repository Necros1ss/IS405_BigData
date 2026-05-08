"""Unit tests for train_spark_v2_fixed module."""
import unittest
from pyspark.sql import SparkSession
from app.train_spark_v2_fixed import train_spark_model


class TestTrainSpark(unittest.TestCase):
    """Test cases for updated regression training."""

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

    def _create_test_data(self, num_rows=120):
        """Create test dataframe with all required feature columns."""
        data = []
        for i in range(num_rows):
            day = (i % 28) + 1
            month = 1 + ((i // 28) % 6)
            date_str = f"2024-{month:02d}-{day:02d}"
            data.append((
                f"v{i}",
                date_str,
                "US" if i % 2 == 0 else "IN",
                float(50 + i),
                float(10 + i * 0.5),
                float(5 + i * 0.2),
                float(0.02 + (i % 10) * 0.01),
                float(0.01 + (i % 5) * 0.005),
                float(20 + i % 60),
                float(80 + i % 500),
                float(1 + i % 12),
                float(1 + i % 7),
                float(2 + i % 8),
                float(3 + i % 10),
            ))
        return self.spark.createDataFrame(
            data,
            [
                "video_id", "parsed_trending_date", "country",
                "log_views", "log_likes", "log_comment_count",
                "like_ratio", "comment_ratio",
                "title_length", "description_length", "tag_count",
                "publish_hour", "publish_day_of_week", "trending_days",
            ],
        )

    def test_train_spark_model_returns_tuple(self):
        df = self._create_test_data(80)
        model, predictions, metrics = train_spark_model(df, num_trees=5, max_depth=3)

        self.assertIsNotNone(model)
        self.assertIsNotNone(predictions)
        self.assertIsNotNone(metrics)
        self.assertIsInstance(metrics, dict)

    def test_model_has_regression_metrics(self):
        df = self._create_test_data(80)
        _, _, metrics = train_spark_model(df, num_trees=5, max_depth=3)

        self.assertIn("rmse", metrics)
        self.assertIn("mae", metrics)
        self.assertIn("r2", metrics)
        self.assertGreaterEqual(metrics["rmse"], 0.0)
        self.assertGreaterEqual(metrics["mae"], 0.0)

    def test_predictions_have_expected_columns(self):
        df = self._create_test_data(90)
        _, predictions, _ = train_spark_model(df, num_trees=5, max_depth=3)

        self.assertIn("prediction", predictions.columns)
        self.assertIn("predicted_days", predictions.columns)
        self.assertIn("trending_days", predictions.columns)

    def test_temporal_split_not_empty(self):
        df = self._create_test_data(120)
        _, predictions, _ = train_spark_model(df, num_trees=5, max_depth=3)
        self.assertGreater(predictions.count(), 0)

    def test_different_hyperparameters(self):
        df = self._create_test_data(100)
        _, _, metrics1 = train_spark_model(df, num_trees=5, max_depth=3)
        _, _, metrics2 = train_spark_model(df, num_trees=10, max_depth=5)

        self.assertIn("rmse", metrics1)
        self.assertIn("rmse", metrics2)


if __name__ == '__main__':
    unittest.main()
