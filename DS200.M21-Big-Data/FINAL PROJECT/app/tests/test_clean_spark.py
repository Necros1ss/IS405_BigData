"""Unit tests for clean_spark_v2_fixed module."""
import unittest
from pyspark.sql import SparkSession
from app.clean_spark_v2_fixed import (
    normalize_input_columns,
    apply_shared_feature_engineering,
    parse_trending_date,
)


class TestCleanSpark(unittest.TestCase):
    """Test cases for new Kaggle schema normalization and feature engineering."""

    @classmethod
    def setUpClass(cls):
        """Initialize Spark session for tests."""
        cls.spark = SparkSession.builder \
            .appName("TestCleanSpark") \
            .master("local[1]") \
            .getOrCreate()
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        """Stop Spark session after tests."""
        cls.spark.stop()

    def test_normalize_input_columns_alias_mapping(self):
        """New schema aliases should map into canonical names."""
        df = self.spark.createDataFrame([
            ("v1", "Title", "Channel A", "22", "2024-01-01T10:00:00Z", "2024-01-02", 1000.0, 100.0, 20.0, "t1|t2", "desc"),
        ], [
            "video_id", "title", "channelTitle", "categoryId", "publishedAt",
            "trending_date", "view_count", "likes", "comment_count", "tags", "description",
        ])

        result = normalize_input_columns(df)

        self.assertIn("channel_title", result.columns)
        self.assertIn("category_id", result.columns)
        self.assertIn("publish_time", result.columns)
        self.assertIn("views", result.columns)
        self.assertEqual(result.select("views").first()[0], 1000.0)

    def test_parse_trending_date(self):
        df = self.spark.createDataFrame([
            ("v1", "24.07.12"),
            ("v2", "2024-07-12"),
        ], ["video_id", "trending_date"])
        normalized = normalize_input_columns(df)
        parsed = parse_trending_date(normalized)
        self.assertEqual(parsed.filter(parsed.parsed_trending_date.isNotNull()).count(), 2)

    def test_feature_engineering_expected_columns(self):
        df = self.spark.createDataFrame([
            ("v1", "Title test", "desc text", "a|b|c", "2024-01-01T10:00:00Z", "2024-01-02", 2000.0, 200.0, 30.0),
        ], [
            "video_id", "title", "description", "tags", "publish_time",
            "trending_date", "views", "likes", "comment_count",
        ])
        normalized = normalize_input_columns(df)
        parsed = parse_trending_date(normalized)
        featured = apply_shared_feature_engineering(parsed)

        expected_cols = {
            "log_views", "log_likes", "log_comment_count",
            "like_ratio", "comment_ratio",
            "publish_hour", "publish_day_of_week",
            "title_length", "description_length", "tag_count",
        }
        self.assertTrue(expected_cols.issubset(set(featured.columns)))

    def test_feature_engineering_ratios(self):
        df = self.spark.createDataFrame([
            ("v1", "t", "d", "x|y", "2024-01-01T10:00:00Z", "2024-01-02", 1000.0, 100.0, 50.0),
        ], [
            "video_id", "title", "description", "tags", "publish_time",
            "trending_date", "views", "likes", "comment_count",
        ])
        normalized = normalize_input_columns(df)
        parsed = parse_trending_date(normalized)
        featured = apply_shared_feature_engineering(parsed)
        row = featured.first()

        self.assertAlmostEqual(row["like_ratio"], 0.1, places=6)
        self.assertAlmostEqual(row["comment_ratio"], 0.05, places=6)

    def test_missing_columns_are_added(self):
        df = self.spark.createDataFrame([
            ("v1", "2024-01-02"),
        ], ["video_id", "trending_date"])
        result = normalize_input_columns(df)

        self.assertIn("likes", result.columns)
        self.assertIn("tags", result.columns)
        self.assertIn("publish_time", result.columns)


if __name__ == '__main__':
    unittest.main()
