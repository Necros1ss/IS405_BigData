"""
Kafka streaming module for real-time YouTube trending duration prediction (Regression).
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
        spark_home = os.environ.get("SPARK_HOME", "/home/thinh/spark")
        if os.path.exists(spark_home):
            os.environ["SPARK_HOME"] = spark_home
            sys.path.insert(0, os.path.join(spark_home, "python"))
            sys.path.insert(0, os.path.join(spark_home, "python", "lib", "py4j-0.10.9.9-src.zip"))

    def build_spark_session(self, app_name="YouTubeTrending_Regression_Stream"):
        try:
            from pyspark.sql import SparkSession
            self.spark = SparkSession.builder.appName(app_name) \
                .config("spark.sql.streaming.metricsEnabled", "false").getOrCreate()
            self.spark.sparkContext.setLogLevel("WARN")
            print(f"✓ Spark session created: {app_name}")
            return True
        except ImportError:
            self._append_local_spark_paths()
            try:
                from pyspark.sql import SparkSession
                self.spark = SparkSession.builder.appName(app_name).getOrCreate()
                self.spark.sparkContext.setLogLevel("WARN")
                return True
            except Exception as exc:
                print(f"⚠ PySpark not available: {exc}")
                return False

    def load_model(self):
        if not self.model_path: return False
        try:
            from pyspark.ml import PipelineModel
            self.model = PipelineModel.load(self.model_path)
            print(f"✓ Regression Model loaded from {self.model_path}")
            return True
        except Exception as e:
            print(f"✗ Failed to load model: {e}")
            return False

    def read_from_kafka(self):
        try:
            df = self.spark.readStream.format("kafka") \
                .option("kafka.bootstrap.servers", self.kafka_servers) \
                .option("subscribe", self.input_topic) \
                .option("startingOffsets", "latest") \
                .option("failOnDataLoss", "false").load()
            return df
        except Exception as e:
            print(f"✗ Failed to create Kafka stream: {e}")
            return None

    def process_stream(self, df):
        try:
            from pyspark.sql import functions as F
            from pyspark.sql.types import StructType, StructField, DoubleType, StringType, IntegerType
            
            raw_schema = StructType([
                StructField("video_id", StringType(), True),
                StructField("title", StringType(), True),
                StructField("publish_date", StringType(), True),
                StructField("video_tags", StringType(), True),
                StructField("views", DoubleType(), True),
                StructField("likes", DoubleType(), True),
                StructField("dislikes", DoubleType(), True),
                StructField("comment_count", DoubleType(), True),
                StructField("comments_disabled", IntegerType(), True),
                StructField("ratings_disabled", IntegerType(), True),
                StructField("video_error_or_removed", IntegerType(), True)
            ])
            
            parsed = df.select(F.from_json(F.col("value").cast("string"), raw_schema).alias("data")).select("data.*")

            df_features = parsed.withColumn("title_length", F.when(F.col("title").isNull(), F.lit(0)).otherwise(F.length(F.col("title")).cast(DoubleType()))) \
                                .withColumn("tag_count", F.when(F.col("video_tags").isNull(), F.lit(0)).otherwise(F.size(F.split(F.col("video_tags"), "\\|")).cast(DoubleType())))
            
            df_features = df_features.withColumn("publish_date_parsed", F.expr("TRY_CAST(publish_date AS timestamp)")) \
                                  .withColumn("publish_hour", F.when(F.col("publish_date_parsed").isNull(), F.lit(12.0)).otherwise(F.hour(F.col("publish_date_parsed")).cast(DoubleType()))) \
                                  .withColumn("publish_dow", F.when(F.col("publish_date_parsed").isNull(), F.lit(1.0)).otherwise(F.dayofweek(F.col("publish_date_parsed")).cast(DoubleType())))

            final_df = df_features.select(
                "video_id", "title", "publish_date", 
                "views", "likes", "dislikes", "comment_count",
                "title_length", "tag_count", "publish_hour", "publish_dow",
                "comments_disabled", "ratings_disabled", "video_error_or_removed"
            )
            return final_df
        except Exception as e:
            print(f"✗ Failed to process stream: {e}")
            return None

    def make_predictions(self, df):
        from pyspark.sql import functions as F
        if not self.model: return df.withColumn("predicted_trending_days", F.lit(None).cast("double"))
        try:
            predictions = self.model.transform(df)
            predictions = predictions \
                .withColumn("predicted_trending_days", F.round(F.col("prediction"), 2)) \
                .select("video_id", "publish_date", "title", "views", "predicted_trending_days")
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
                query = output.writeStream.format("console").option("truncate", False) \
                    .option("checkpointLocation", f"{self.checkpoint_dir}_console").start()
            else:
                query = output.writeStream.format("kafka").option("kafka.bootstrap.servers", self.kafka_servers) \
                    .option("topic", self.output_topic).option("checkpointLocation", self.checkpoint_dir).start()
            return query
        except Exception as e:
            print(f"✗ Failed to write output: {e}")
            return None

    def run(self, output_mode="console"):
        print(f"\n=== Starting Regression Kafka Stream ===")
        if not self.build_spark_session(): return False
        if self.model_path: self.load_model()
        
        df_stream = self.read_from_kafka()
        if df_stream is None: return False
        
        df_processed = self.process_stream(df_stream)
        df_predictions = self.make_predictions(df_processed)
        query = self.write_to_kafka(df_predictions, mode=output_mode)
        
        print("\n⏳ Streaming pipeline running. Press Ctrl+C to stop...\n")
        try:
            query.awaitTermination()
        except KeyboardInterrupt:
            print("\n✓ Streaming stopped")
            query.stop()
            self.spark.stop()
        return True

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--kafka-servers", default="localhost:9092")
    parser.add_argument("--input-topic", default="youtube_videos")
    parser.add_argument("--output-topic", default="youtube_predictions")
    parser.add_argument("--model-path", help="Path to RandomForestRegressor model")
    parser.add_argument("--checkpoint-dir", default="/home/thinh/spark_checkpoint_reg")
    parser.add_argument("--output", choices=["console", "kafka"], default="console")
    
    args = parser.parse_args()
    pipeline = KafkaStreamingPipeline(args.kafka_servers, args.input_topic, args.output_topic, args.model_path, args.checkpoint_dir)
    pipeline.run(output_mode=args.output)

if __name__ == "__main__":
    main()