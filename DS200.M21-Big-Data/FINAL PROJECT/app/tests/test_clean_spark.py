"""
Unit tests for clean_spark module.
Tests data loading, normalization, and feature engineering.
"""
import unittest
import tempfile
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from app.clean_spark import normalize_input_columns, engineer_features, load_csv_with_spark


class TestCleanSpark(unittest.TestCase):
    """Test cases for clean_spark functions."""

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

    def test_normalize_input_columns_rename(self):
        """Test column renaming."""
        df = self.spark.createDataFrame([
            (10, "tag1|tag2"),
        ], ["like_count", "video_tags"])

        result = normalize_input_columns(df)

        self.assertIn("likes", result.columns)
        self.assertIn("tags", result.columns)
        self.assertNotIn("like_count", result.columns)

    def test_normalize_input_columns_add_missing(self):
        """Test adding missing columns."""
        df = self.spark.createDataFrame([
            (100, "description"),
        ], ["view_count", "description"])

        result = normalize_input_columns(df)

        self.assertIn("tags", result.columns)
        self.assertIn("likes", result.columns)

    def test_engineer_features_creates_columns(self):
        """Test feature engineering creates expected columns."""
        df = self.spark.createDataFrame([
            (1000, 50, 5, "tag1|tag2|tag3", "this is a description"),
        ], ["view_count", "likes", "comment_count", "tags", "description"])

        result = engineer_features(df)

        expected_cols = {"tag_count", "description_length", "like_ratio", "comment_ratio", "engagement"}
        self.assertTrue(expected_cols.issubset(set(result.columns)))

    def test_engineer_features_calculates_ratios(self):
        """Test that ratios are calculated correctly."""
        df = self.spark.createDataFrame([
            (1000, 100, 50, "tag1|tag2", "desc"),
        ], ["view_count", "likes", "comment_count", "tags", "description"])

        result = engineer_features(df)
        rows = result.collect()

        self.assertEqual(len(rows), 1)
        row = rows[0]
        # like_ratio = 100 / (1000 + 1) ≈ 0.0999
        self.assertAlmostEqual(row["like_ratio"], 100.0 / 1001.0, places=3)
        # comment_ratio = 50 / (1000 + 1) ≈ 0.0499
        self.assertAlmostEqual(row["comment_ratio"], 50.0 / 1001.0, places=3)
        # engagement = 100 + 50 = 150
        self.assertEqual(row["engagement"], 150.0)

    def test_engineer_features_tag_count(self):
        """Test tag count calculation."""
        df = self.spark.createDataFrame([
            (1000, 10, 5, "tag1|tag2|tag3|tag4", "desc"),
        ], ["view_count", "likes", "comment_count", "tags", "description"])

        result = engineer_features(df)
        rows = result.collect()

        self.assertEqual(rows[0]["tag_count"], 4)

    def test_engineer_features_description_length(self):
        """Test description length calculation."""
        desc = "this is a test description"
        df = self.spark.createDataFrame([
            (1000, 10, 5, "tag1", desc),
        ], ["view_count", "likes", "comment_count", "tags", "description"])

        result = engineer_features(df)
        rows = result.collect()

        self.assertEqual(rows[0]["description_length"], len(desc))

    def test_engineer_features_drops_nulls(self):
        """Test that rows with nulls are dropped."""
        df = self.spark.createDataFrame([
            (1000, 10, 5, None, "desc"),  # null tags
            (2000, 20, 10, "tag1|tag2", "desc"),  # valid
        ], ["view_count", "likes", "comment_count", "tags", "description"])

        result = engineer_features(df)
        self.assertEqual(result.count(), 1)

    def test_load_csv_with_spark(self):
        """Test CSV loading functionality."""
        # Create temporary CSV file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("view_count,likes,comment_count,tags,description\n")
            f.write("1000,50,5,tag1|tag2,test description\n")
            f.write("2000,100,10,tag1|tag2|tag3,another description\n")
            temp_file = f.name

        try:
            df = load_csv_with_spark(self.spark, temp_file)
            self.assertEqual(df.count(), 2)
            self.assertIn("view_count", df.columns)
        finally:
            os.unlink(temp_file)


if __name__ == '__main__':
    unittest.main()
