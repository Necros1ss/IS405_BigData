import argparse
import os
import sys

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
        self.spark = SparkSession.builder.appName("YouTubeStreaming") \
            .config("spark.sql.streaming.metricsEnabled", "false").getOrCreate()
        self.spark.sparkContext.setLogLevel("WARN")
        return True

    def load_model(self):
        from pyspark.ml import PipelineModel
        if self.model_path:
            self.model = PipelineModel.load(self.model_path)
            return True
        return False

    def process_stream(self, df):
        from pyspark.sql import functions as F
        from pyspark.sql.types import StructType, StructField, DoubleType, StringType
        
        raw_schema = StructType([
            StructField("video_id", StringType(), True),
            StructField("title", StringType(), True),
            StructField("publish_date", StringType(), True),
            StructField("video_tags", StringType(), True),
            StructField("views", DoubleType(), True),
            StructField("likes", DoubleType(), True),
            StructField("comment_count", DoubleType(), True)
        ])
        
        parsed = df.select(F.from_json(F.col("value").cast("string"), raw_schema).alias("data")).select("data.*")

        df_features = parsed.withColumn("title_length", F.when(F.col("title").isNull(), F.lit(0)).otherwise(F.length(F.col("title")).cast(DoubleType()))) \
                            .withColumn("tag_count", F.when(F.col("video_tags").isNull(), F.lit(0)).otherwise(F.size(F.split(F.col("video_tags"), "\\|")).cast(DoubleType())))
        
        df_features = df_features.withColumn("publish_date_parsed", F.expr("TRY_CAST(publish_date AS timestamp)")) \
                              .withColumn("publish_hour", F.when(F.col("publish_date_parsed").isNull(), F.lit(12.0)).otherwise(F.hour(F.col("publish_date_parsed")).cast(DoubleType()))) \
                              .withColumn("publish_dow", F.when(F.col("publish_date_parsed").isNull(), F.lit(1.0)).otherwise(F.dayofweek(F.col("publish_date_parsed")).cast(DoubleType())))

        # TÁI TẠO LOG-FEATURES CHO LUỒNG STREAM
        df_features = df_features \
            .withColumn("log_views", F.log1p(F.col("views"))) \
            .withColumn("log_likes", F.log1p(F.col("likes"))) \
            .withColumn("log_comment_count", F.log1p(F.col("comment_count"))) \
            .withColumn("like_ratio", F.when(F.col("views") == 0, 0.0).otherwise(F.col("likes") / F.col("views"))) \
            .withColumn("comment_ratio", F.when(F.col("views") == 0, 0.0).otherwise(F.col("comment_count") / F.col("views")))

        final_df = df_features.select(
            "video_id", "title", "views",
            "log_views", "log_likes", "log_comment_count", "like_ratio", "comment_ratio",
            "title_length", "tag_count", "publish_hour", "publish_dow"
        )
        return final_df

    def make_predictions(self, df):
        from pyspark.sql import functions as F
        if not self.model: return df.withColumn("predicted_trending_days", F.lit(None).cast("double"))
        
        predictions = self.model.transform(df)
        # ĐẢO NGƯỢC LOG (expm1) TRONG REAL-TIME
        predictions = predictions \
            .withColumn("predicted_trending_days", F.round(F.expr("expm1(prediction)"), 2)) \
            .select("video_id", "title", "views", "predicted_trending_days")
        return predictions

    def run(self, output_mode="console"):
        self.build_spark_session()
        self.load_model()
        df_stream = self.spark.readStream.format("kafka").option("kafka.bootstrap.servers", self.kafka_servers).option("subscribe", self.input_topic).load()
        df_processed = self.process_stream(df_stream)
        df_predictions = self.make_predictions(df_processed)
        
        from pyspark.sql import functions as F
        output = df_predictions.select(F.to_json(F.struct("*")).alias("value"))
        query = output.writeStream.format("kafka").option("kafka.bootstrap.servers", self.kafka_servers).option("topic", self.output_topic).option("checkpointLocation", self.checkpoint_dir).start()
        
        print("⏳ Streaming running...")
        query.awaitTermination()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--kafka-servers", default="localhost:9092")
    parser.add_argument("--input-topic", default="youtube_videos")
    parser.add_argument("--output-topic", default="youtube_predictions")
    parser.add_argument("--model-path", default="models/rf_regression_model")
    parser.add_argument("--checkpoint-dir", default="/home/thinh/spark_chkpt")
    args = parser.parse_args()
    KafkaStreamingPipeline(args.kafka_servers, args.input_topic, args.output_topic, args.model_path, args.checkpoint_dir).run(output_mode="kafka")