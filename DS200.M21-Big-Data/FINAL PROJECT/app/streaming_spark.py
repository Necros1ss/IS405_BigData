import argparse
import os
import sys
import logging

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from app.clean_spark_v2_fixed import get_shared_schema, apply_shared_feature_engineering
from app.clean_spark_v2_fixed import normalize_input_columns, parse_trending_date

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class KafkaStreamingPipeline:
    def __init__(self, kafka_servers, input_topic, output_topic, model_path, checkpoint_dir):
        self.kafka_servers = kafka_servers
        self.input_topic = input_topic
        self.output_topic = output_topic
        self.model_path = model_path
        self.checkpoint_dir = checkpoint_dir
        self.spark = None
        self.model = None

    def build_spark_session(self):
        from pyspark.sql import SparkSession
        self.spark = SparkSession.builder.appName("YouTubeStreaming_Prod") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1") \
            .config("spark.sql.streaming.metricsEnabled", "false") \
            .config("spark.sql.shuffle.partitions", "10") \
            .getOrCreate()
        self.spark.sparkContext.setLogLevel("WARN")
        logger.info("✓ Khởi tạo Spark Streaming thành công")

    def load_model(self):
        from pyspark.ml import PipelineModel
        try:
            self.model = PipelineModel.load(self.model_path)
            logger.info(f"✓ Đã load mô hình từ {self.model_path}")
        except Exception as e:
            logger.error(f"✗ Không thể load mô hình: {e}")
            sys.exit(1)

    def process_stream(self, df):
        from pyspark.sql import functions as F
        raw_schema = get_shared_schema()

        parsed = df.select(F.from_json(F.col("value").cast("string"), raw_schema).alias("data")).select("data.*")

        parsed = normalize_input_columns(parsed)
        parsed = parsed.withColumn(
            "trending_date",
            F.when(F.col("trending_date") == "", F.date_format(F.current_date(), "yyyy-MM-dd")).otherwise(F.col("trending_date")),
        )
        parsed = parse_trending_date(parsed)
        parsed = parsed.withColumn(
            "parsed_trending_date",
            F.coalesce(F.col("parsed_trending_date"), F.current_date().cast("date")),
        )

        df_features = apply_shared_feature_engineering(parsed)

        return df_features.select(
            "video_id", "title", "views", "country",
            "log_views", "log_likes", "log_comment_count", "like_ratio", "comment_ratio",
            "title_length", "description_length", "tag_count",
            "publish_hour", "publish_day_of_week"
        )

    def make_predictions(self, df):
        from pyspark.sql import functions as F
        predictions = self.model.transform(df)
        predictions = predictions \
            .withColumn("predicted_trending_days", F.round(F.greatest(F.col("prediction"), F.lit(0.0)), 2)) \
            .select("video_id", "title", "views", "predicted_trending_days")
        return predictions

    def run(self):
        self.build_spark_session()
        self.load_model()
        
        df_stream = self.spark.readStream.format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_servers) \
            .option("subscribe", self.input_topic).load()
            
        df_processed = self.process_stream(df_stream)
        df_predictions = self.make_predictions(df_processed)
        
        from pyspark.sql import functions as F
        output = df_predictions.select(F.to_json(F.struct("*")).alias("value"))
        
        logger.info(f"⏳ Đang chạy luồng Streaming đẩy sang {self.output_topic}...")
        query = output.writeStream.format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_servers) \
            .option("topic", self.output_topic) \
            .option("checkpointLocation", self.checkpoint_dir) \
            .start()
        
        query.awaitTermination()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--kafka-servers", default="localhost:9092")
    parser.add_argument("--input-topic", default="youtube_videos")
    parser.add_argument("--output-topic", default="youtube_predictions")
    parser.add_argument("--model-path", default="models/rf_regression_model")
    parser.add_argument("--checkpoint-dir", default="hdfs://localhost:9000/user/thinh/spark_chkpt")
    args = parser.parse_args()
    
    KafkaStreamingPipeline(args.kafka_servers, args.input_topic, args.output_topic, args.model_path, args.checkpoint_dir).run()