"""
Kafka streaming module for real-time YouTube trending prediction (V2).
"""
import argparse
import os
import sys
from typing import Optional


class KafkaStreamingPipeline:
    def __init__(
        self,
        kafka_servers: str = "localhost:9092",
        input_topic: str = "youtube_videos",
        output_topic: str = "youtube_predictions",
        model_path: Optional[str] = None,
        checkpoint_dir: str = "/home/thinh/spark_checkpoint"
    ):
        self.kafka_servers = kafka_servers
        self.input_topic = input_topic
        self.output_topic = output_topic
        self.model_path = model_path
        self.checkpoint_dir = checkpoint_dir
        self.spark = None
        self.model = None

    def _append_local_spark_paths(self):
        spark_home = os.environ.get("SPARK_HOME")
        if not spark_home:
            for candidate in ("/home/thinh/spark", os.path.expanduser("~/spark")):
                if os.path.isdir(candidate):
                    spark_home = candidate
                    os.environ["SPARK_HOME"] = candidate
                    break

        if not spark_home:
            return

        spark_python = os.path.join(spark_home, "python")
        spark_pyspark_zip = os.path.join(spark_home, "python", "lib", "pyspark.zip")
        spark_py4j = os.path.join(spark_home, "python", "lib", "py4j-0.10.9.9-src.zip")

        for path in (spark_python, spark_pyspark_zip, spark_py4j):
            if os.path.exists(path) and path not in sys.path:
                sys.path.insert(0, path)

    def build_spark_session(self, app_name: str = "YouTubeTrendingKafkaStreamV2"):
        try:
            from pyspark.sql import SparkSession
            builder = SparkSession.builder.appName(app_name)
            builder = builder.config("spark.sql.streaming.metricsEnabled", "false")
            kafka_packages = os.environ.get("SPARK_KAFKA_PACKAGES")
            if kafka_packages:
                builder = builder.config("spark.jars.packages", kafka_packages)
            self.spark = builder.getOrCreate()
            self.spark.sparkContext.setLogLevel("WARN")
            print(f"✓ Spark session created: {app_name}")
        except ImportError:
            self._append_local_spark_paths()
            try:
                from pyspark.sql import SparkSession
                builder = SparkSession.builder.appName(app_name)
                builder = builder.config("spark.sql.streaming.metricsEnabled", "false")
                kafka_packages = os.environ.get("SPARK_KAFKA_PACKAGES")
                if kafka_packages:
                    builder = builder.config("spark.jars.packages", kafka_packages)
                self.spark = builder.getOrCreate()
                self.spark.sparkContext.setLogLevel("WARN")
                print(f"✓ Spark session created: {app_name}")
            except Exception as exc:
                print(f"⚠ PySpark not available: {exc}")
                print("Install or expose Spark Python libs before running the stream.")
                return False
        return True

    def load_model(self):
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
        if not self.spark:
            print("✗ Spark session not initialized")
            return None

        try:
            df = self.spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.kafka_servers) \
                .option("subscribe", self.input_topic) \
                .option("startingOffsets", "earliest") \
                .option("failOnDataLoss", "false") \
                .load()
            
            print(f"✓ Kafka stream created: {self.input_topic}")
            return df
        except Exception as e:
            print(f"✗ Failed to create Kafka stream: {e}")
            return None

    def process_stream(self, df):
        """
        Process streaming data: apply V2 feature engineering exactly like training.
        """
        try:
            from pyspark.sql import functions as F
            from pyspark.sql.types import StructType, StructField, DoubleType, StringType
            
            # 1. Khai báo Schema nhận từ Producer (Chỉ có raw data)
            raw_schema = StructType([
                StructField("video_id", StringType(), True),
                StructField("title", StringType(), True),
                StructField("description", StringType(), True),
                StructField("video_tags", StringType(), True),
                StructField("publish_date", StringType(), True),
                StructField("language", StringType(), True)
            ])
            
            # 2. Parse JSON
            parsed = df.select(
                F.from_json(F.col("value").cast("string"), raw_schema).alias("data")
            ).select("data.*")

            # 3. Tạo Features V2
            df_features = parsed.withColumn("title_length", 
                                  F.when(F.col("title").isNull(), F.lit(0))
                                  .otherwise(F.length(F.col("title")).cast(DoubleType()))) \
                                .withColumn("description_length", 
                                  F.when(F.col("description").isNull(), F.lit(0))
                                  .otherwise(F.length(F.col("description")).cast(DoubleType())))
            
            df_features = df_features.withColumn("tag_count", 
                                  F.when(F.col("video_tags").isNull(), F.lit(0))
                                  .otherwise(F.when(F.col("video_tags") == "", F.lit(0))
                                             .otherwise(F.size(F.split(F.col("video_tags"), "\\|")).cast(DoubleType()))))
            
            df_features = df_features.withColumn("publish_date_parsed", F.expr("TRY_CAST(publish_date AS timestamp)")) \
                                  .withColumn("publish_hour", 
                                  F.when(F.col("publish_date_parsed").isNull(), F.lit(12.0))
                                  .otherwise(F.hour(F.col("publish_date_parsed")).cast(DoubleType()))) \
                                  .withColumn("publish_day_of_week", 
                                  F.when(F.col("publish_date_parsed").isNull(), F.lit(1.0))
                                  .otherwise(F.dayofweek(F.col("publish_date_parsed")).cast(DoubleType())))
            
            df_features = df_features.withColumn("is_english",
                                  F.when(F.lower(F.col("language")).like("%english%"), F.lit(1.0))
                                  .otherwise(F.lit(0.0)))

            # Giữ lại các cột cần thiết cho model dự đoán
            final_df = df_features.select(
                "video_id", "title", "publish_date", 
                "title_length", "description_length", "tag_count", 
                "publish_hour", "publish_day_of_week", "is_english"
            )
            
            print("✓ Stream processing (V2) configured")
            return final_df
        except Exception as e:
            print(f"✗ Failed to process stream: {e}")
            return None

    def make_predictions(self, df):
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
            
            # Đã bỏ cột engagement đi, thay bằng publish_hour để quan sát
            predictions = predictions \
                .withColumn("trending", F.col("prediction").cast("integer")) \
                .withColumn("probability_array", vector_to_array(F.col("probability"))) \
                .withColumn("prob_not_trending", F.round(F.col("probability_array")[0], 4)) \
                .withColumn("prob_trending", F.round(F.col("probability_array")[1], 4)) \
                .select("video_id", "publish_date", "title", "trending", 
                    "prob_not_trending", "prob_trending")
            
            print("✓ Predictions applied to stream")
            return predictions
        except Exception as e:
            print(f"✗ Failed to make predictions: {e}")
            return df

    def write_to_kafka(self, df, mode="console"):
        try:
            from pyspark.sql import functions as F
            import os
            
            os.makedirs(self.checkpoint_dir, exist_ok=True)
            output = df.select(F.to_json(F.struct("*")).alias("value"))
            
            if mode == "console":
                query = output.writeStream \
                    .format("console") \
                    .option("truncate", False) \
                    .option("checkpointLocation", f"{self.checkpoint_dir}_console") \
                    .start()
                print(f"✓ Writing predictions to console")
            else:
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
        
        df_stream = self.read_from_kafka()
        if df_stream is None: return False
        
        df_processed = self.process_stream(df_stream)
        if df_processed is None: return False
        
        df_predictions = self.make_predictions(df_processed)
        
        query = self.write_to_kafka(df_predictions, mode=output_mode)
        if query is None: return False
        
        print("\n⏳ Streaming pipeline running. Press Ctrl+C to stop...\n")
        
        try:
            query.awaitTermination()
        except KeyboardInterrupt:
            print("\n✓ Streaming stopped by user")
            query.stop()
            self.spark.stop()
        
        return True

def main():
    parser = argparse.ArgumentParser(description="Kafka streaming pipeline for YouTube trending prediction")
    parser.add_argument("--kafka-servers", default="localhost:9092", help="Kafka broker addresses")
    parser.add_argument("--input-topic", default="youtube_videos", help="Input Kafka topic")
    parser.add_argument("--output-topic", default="youtube_predictions", help="Output Kafka topic")
    parser.add_argument("--model-path", help="Path to pre-trained RandomForest model (HDFS or local)")
    parser.add_argument("--checkpoint-dir", default="/home/thinh/spark_checkpoint", help="Checkpoint directory")
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