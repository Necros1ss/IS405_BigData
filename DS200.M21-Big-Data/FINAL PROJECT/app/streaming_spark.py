"""
Kafka streaming module for real-time YouTube trending prediction.
This is a scaffold for future implementation.

Usage:
  python3 -m app.streaming_spark --kafka-servers localhost:9092 --input-topic youtube_videos --output-topic predictions
"""
import argparse
import os
from typing import Optional


class KafkaStreamingPipeline:
    """
    Kafka streaming pipeline for real-time predictions.
    
    This scaffold provides the structure for:
    1. Reading from Kafka topic (raw video data)
    2. Applying Spark transformations (feature engineering)
    3. Loading pre-trained model (RandomForest)
    4. Making predictions
    5. Writing results to output Kafka topic
    
    Future phases will implement full functionality.
    """

    def __init__(
        self,
        kafka_servers: str = "localhost:9092",
        input_topic: str = "youtube_videos",
        output_topic: str = "youtube_predictions",
        model_path: Optional[str] = None,
        checkpoint_dir: str = "/tmp/spark_checkpoint"
    ):
        """
        Initialize streaming pipeline.
        
        Args:
            kafka_servers: Kafka broker addresses (comma-separated)
            input_topic: Kafka topic to read from
            output_topic: Kafka topic to write predictions to
            model_path: Path to pre-trained RandomForest model (HDFS or local)
            checkpoint_dir: Checkpoint directory for fault tolerance
        """
        self.kafka_servers = kafka_servers
        self.input_topic = input_topic
        self.output_topic = output_topic
        self.model_path = model_path
        self.checkpoint_dir = checkpoint_dir
        self.spark = None
        self.model = None

    def build_spark_session(self, app_name: str = "YouTubeTrendingKafkaStream"):
        """Build Spark session with Kafka support."""
        try:
            from pyspark.sql import SparkSession
            builder = SparkSession.builder.appName(app_name)
            kafka_packages = os.environ.get("SPARK_KAFKA_PACKAGES")
            if kafka_packages:
                builder = builder.config("spark.jars.packages", kafka_packages)
            self.spark = builder.getOrCreate()
            print(f"✓ Spark session created: {app_name}")
        except ImportError:
            print("⚠ PySpark not available. Install: pip install pyspark")
            return False
        return True

    def load_model(self):
        """Load pre-trained model from path."""
        if not self.model_path:
            print("⚠ No model path specified. Predictions will not work.")
            return False

        try:
            from pyspark.ml import PipelineModel
            self.model = PipelineModel.load(self.model_path)
            print(f"✓ Model loaded from {self.model_path}")
        except Exception as e:
            print(f"✗ Failed to load model: {e}")
            return False
        return True

    def read_from_kafka(self):
        """
        Read streaming data from Kafka.
        
        Expected message format (JSON):
        {
            "video_id": "...",
            "tag_count": 30,
            "description_length": 800,
            "like_ratio": 0.10,
            "comment_ratio": 0.05,
            "engagement": 25000
        }
        """
        if not self.spark:
            print("✗ Spark session not initialized")
            return None

        try:
            df = self.spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.kafka_servers) \
                .option("subscribe", self.input_topic) \
                .option("startingOffsets", "latest") \
                .option("failOnDataLoss", "false") \
                .load()
            
            print(f"✓ Kafka stream created: {self.input_topic}")
            return df
        except Exception as e:
            print(f"✗ Failed to create Kafka stream: {e}")
            return None

    def process_stream(self, df):
        """
        Process streaming data: deserialize JSON, apply transformations & feature engineering.
        
        Accepts 2 formats:
        1. Raw video data: {video_id, title, view_count, like_count, comment_count, video_tags, description}
        2. Pre-engineered: {video_id, tag_count, description_length, like_ratio, comment_ratio, engagement}
        
        Args:
            df: Spark streaming DataFrame from Kafka
        
        Returns:
            Processed streaming DataFrame with engineered features
        """
        try:
            from pyspark.sql import functions as F
            from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType, StringType
            
            raw_schema = StructType([
                StructField("video_id", StringType(), True),
                StructField("event_time", StringType(), True),
                StructField("title", StringType(), True),
                StructField("view_count", IntegerType(), True),
                StructField("like_count", IntegerType(), True),
                StructField("comment_count", IntegerType(), True),
                StructField("video_tags", StringType(), True),
                StructField("description", StringType(), True),
            ])

            engineered_schema = StructType([
                StructField("video_id", StringType(), True),
                StructField("event_time", StringType(), True),
                StructField("title", StringType(), True),
                StructField("tag_count", IntegerType(), True),
                StructField("description_length", IntegerType(), True),
                StructField("like_ratio", DoubleType(), True),
                StructField("comment_ratio", DoubleType(), True),
                StructField("engagement", DoubleType(), True),
            ])
            
            parsed = df.select(
                F.from_json(F.col("value").cast("string"), raw_schema).alias("raw"),
                F.from_json(F.col("value").cast("string"), engineered_schema).alias("engineered")
            )

            df_parsed = parsed.select(
                F.coalesce(F.col("raw.video_id"), F.col("engineered.video_id")).alias("video_id"),
                F.coalesce(F.col("raw.event_time"), F.col("engineered.event_time")).alias("event_time"),
                F.coalesce(F.col("raw.title"), F.col("engineered.title")).alias("title"),
                F.coalesce(F.col("engineered.tag_count"), F.when(F.col("raw.video_tags").isNotNull(), F.size(F.split(F.col("raw.video_tags"), "\\|")))).alias("tag_count"),
                F.coalesce(F.col("engineered.description_length"), F.when(F.col("raw.description").isNotNull(), F.length(F.col("raw.description")))).alias("description_length"),
                F.coalesce(F.col("engineered.like_ratio"), F.when(F.col("raw.view_count") > 0, F.col("raw.like_count") / F.col("raw.view_count")).otherwise(F.lit(0.0))).alias("like_ratio"),
                F.coalesce(F.col("engineered.comment_ratio"), F.when(F.col("raw.view_count") > 0, F.col("raw.comment_count") / F.col("raw.view_count")).otherwise(F.lit(0.0))).alias("comment_ratio"),
                F.coalesce(F.col("engineered.engagement"), (F.col("raw.like_count") + F.col("raw.comment_count")).cast(DoubleType())).alias("engagement")
            )

            df_parsed = df_parsed.select(
                "video_id",
                "event_time",
                "title",
                "tag_count",
                "description_length",
                "like_ratio",
                "comment_ratio",
                "engagement"
            )
            
            print("✓ Stream processing configured")
            return df_parsed
        except Exception as e:
            print(f"✗ Failed to process stream: {e}")
            return None

    def make_predictions(self, df):
        """
        Apply model to streaming data.
        
        Args:
            df: Processed streaming DataFrame
        
        Returns:
            DataFrame with predictions (video_id, title, prediction, probability)
        """
        from pyspark.sql import functions as F
        from pyspark.ml.functions import vector_to_array

        if not self.model:
            print("⚠ Model not loaded. Stream will pass through engineered features without predictions.")
            return df \
                .withColumn("trending", F.lit(None).cast("integer")) \
                .withColumn("prob_not_trending", F.lit(None).cast("double")) \
                .withColumn("prob_trending", F.lit(None).cast("double"))

        try:
            # Apply model
            predictions = self.model.transform(df)
            
            # Extract prediction and probability
            predictions = predictions \
                .withColumn("trending", F.col("prediction").cast("integer")) \
                .withColumn("probability_array", vector_to_array(F.col("probability"))) \
                .withColumn("prob_not_trending", F.round(F.col("probability_array")[0], 4)) \
                .withColumn("prob_trending", F.round(F.col("probability_array")[1], 4)) \
                .select("video_id", "event_time", "title", "engagement", "trending", 
                    "prob_not_trending", "prob_trending")
            
            print("✓ Predictions applied to stream")
            return predictions
        except Exception as e:
            print(f"✗ Failed to make predictions: {e}")
            return df

    def write_to_kafka(self, df, mode="console"):
        """
        Write predictions back to Kafka or console (for debugging).
        
        Args:
            df: DataFrame with predictions
            mode: "kafka" or "console" for output destination
        """
        try:
            from pyspark.sql import functions as F
            import os
            
            # Ensure checkpoint dir exists
            os.makedirs(self.checkpoint_dir, exist_ok=True)
            
            # Select columns to write
            output = df.select(
                F.to_json(F.struct("*")).alias("value")
            )
            
            if mode == "console":
                # Console output for debugging
                query = output.writeStream \
                    .format("console") \
                    .option("truncate", False) \
                    .option("checkpointLocation", f"{self.checkpoint_dir}_console") \
                    .start()
                print(f"✓ Writing predictions to console")
            else:
                # Kafka output
                query = output.writeStream \
                    .format("kafka") \
                    .option("kafka.bootstrap.servers", self.kafka_servers) \
                    .option("topic", self.output_topic) \
                    .option("checkpointLocation", self.checkpoint_dir) \
                    .start()
                print(f"✓ Writing predictions to Kafka topic: {self.output_topic}")
            
            return query
        except Exception as e:
            print(f"✗ Failed to write output: {e}")
            return None

    def run(self, output_mode="console"):
        """
        Execute the streaming pipeline.
        
        Steps:
        1. Build Spark session
        2. Load pre-trained model
        3. Read from Kafka
        4. Process and predict
        5. Write results to console or Kafka
        6. Wait for termination
        
        Args:
            output_mode: "console" (for testing) or "kafka" (for production)
        """
        print(f"\n=== Starting Kafka Streaming Pipeline (output: {output_mode}) ===")
        print(f"Input topic: {self.input_topic}")
        print(f"Output topic: {self.output_topic}")
        print(f"Kafka servers: {self.kafka_servers}")
        print()
        
        if not self.build_spark_session():
            return False
        
        if self.model_path and not self.load_model():
            print("⚠ Model path provided but failed to load (predictions may not work)")
        elif not self.model_path:
            print("⚠ No model path provided (predictions disabled)")
        
        # Read stream
        df_stream = self.read_from_kafka()
        if df_stream is None:
            return False
        
        # Process
        df_processed = self.process_stream(df_stream)
        if df_processed is None:
            return False
        
        # Predict
        df_predictions = self.make_predictions(df_processed)
        
        # Write output
        query = self.write_to_kafka(df_predictions, mode=output_mode)
        if query is None:
            return False
        
        print("\n⏳ Streaming pipeline running. Press Ctrl+C to stop...\n")
        
        # Wait for termination (run forever)
        try:
            query.awaitTermination()
        except KeyboardInterrupt:
            print("\n✓ Streaming stopped by user")
            query.stop()
            self.spark.stop()
        
        return True


def main():
    """CLI entry point for streaming pipeline."""
    parser = argparse.ArgumentParser(description="Kafka streaming pipeline for YouTube trending prediction")
    parser.add_argument("--kafka-servers", default="localhost:9092", help="Kafka broker addresses")
    parser.add_argument("--input-topic", default="youtube_videos", help="Input Kafka topic")
    parser.add_argument("--output-topic", default="youtube_predictions", help="Output Kafka topic")
    parser.add_argument("--model-path", help="Path to pre-trained RandomForest model (HDFS or local)")
    parser.add_argument("--checkpoint-dir", default="/tmp/spark_checkpoint", help="Checkpoint directory")
    parser.add_argument("--output", choices=["console", "kafka"], default="console", 
        help="Output destination: console (testing) or kafka (production)")
    
    args = parser.parse_args()
    
    pipeline = KafkaStreamingPipeline(
        kafka_servers=args.kafka_servers,
        input_topic=args.input_topic,
        output_topic=args.output_topic,
        model_path=args.model_path,
        checkpoint_dir=args.checkpoint_dir
    )
    
    pipeline.run(output_mode=args.output)


if __name__ == "__main__":
    main()
