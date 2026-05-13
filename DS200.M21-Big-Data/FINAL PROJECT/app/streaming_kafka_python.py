#!/usr/bin/env python3

import os
import json
import argparse
import pickle
import numpy as np
import pandas as pd
from kafka import KafkaConsumer, KafkaProducer
from datetime import datetime

# ============================================================
# FEATURE ENGINEERING (Pure Python)
# ============================================================
def apply_streaming_features(record):
    """
    Apply feature engineering without Spark.
    Returns a dictionary with all features.
    """
    features = {}
    
    # Handle missing values
    views = float(record.get("views", 0))
    likes = float(record.get("likes", 0))
    comment_count = float(record.get("comment_count", 0))
    
    # Log features
    features["log_views"] = np.log1p(views)
    features["log_likes"] = np.log1p(likes)
    features["log_comment_count"] = np.log1p(comment_count)
    
    # Ratio features
    features["like_ratio"] = likes / views if views > 0 else 0.0
    features["comment_ratio"] = comment_count / views if views > 0 else 0.0
    
    # Text features
    title = record.get("title", "")
    features["title_length"] = float(len(str(title)))
    
    # Country encoding (simple: 0 for US, 1 for others)
    country = record.get("country", "")
    features["country_is_us"] = 1.0 if country == "US" else 0.0
    
    return features

# ============================================================
# MAIN STREAMING
# ============================================================
def run_stream():
    parser = argparse.ArgumentParser()
    parser.add_argument("--kafka-servers", default="localhost:9092")
    parser.add_argument("--input-topic", default="youtube_videos")
    parser.add_argument("--output-topic", default="youtube_predictions")
    parser.add_argument("--model-path", required=True)
    parser.add_argument("--checkpoint-dir", required=True)
    args = parser.parse_args()

    # Try to load the model (pickle format)
    model = None
    model_loaded = False
    
    try:
        model_pkl_path = os.path.join(args.model_path, "model.pkl")
        if os.path.exists(model_pkl_path):
            print(f"Loading model from {model_pkl_path}...")
            with open(model_pkl_path, 'rb') as f:
                model = pickle.load(f)
            model_loaded = True
            print("✓ Model loaded successfully")
        else:
            print(f"⚠ Model pickle file not found at {model_pkl_path}")
            print(f"  Available files in {args.model_path}:")
            for item in os.listdir(args.model_path):
                print(f"    - {item}")
    except Exception as e:
        print(f"⚠ Could not load model: {e}")
        print("  Continuing with dummy predictions...")

    print("=" * 80)
    print("YOUTUBE TRENDING REALTIME PREDICTION (KAFKA - Pure Python)")
    print("=" * 80)

    # Initialize Kafka Consumer
    print(f"Connecting to Kafka (Topic: {args.input_topic})...")
    try:
        consumer = KafkaConsumer(
            args.input_topic,
            bootstrap_servers=args.kafka_servers,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest',
            group_id='youtube-predictions-group'
        )
        print(f"✓ Connected to Kafka topic: {args.input_topic}")
    except Exception as e:
        print(f"✗ Failed to connect to Kafka: {e}")
        raise

    # Initialize Kafka Producer
    producer = KafkaProducer(
        bootstrap_servers=args.kafka_servers,
        value_serializer=lambda m: json.dumps(m).encode('utf-8')
    )
    print(f"✓ Producer initialized for topic: {args.output_topic}")

    print("✓ Streaming started successfully")
    print("=" * 80)
    print("Waiting for messages from Kafka...")

    message_count = 0
    try:
        for message in consumer:
            try:
                record = message.value
                message_count += 1
                
                # Apply feature engineering
                features = apply_streaming_features(record)
                
                # Make prediction
                if model_loaded and model:
                    try:
                        # Convert features to numpy array in correct order
                        feature_names = sorted(features.keys())
                        X = np.array([[features[name] for name in feature_names]])
                        pred_value = float(model.predict(X)[0])
                    except Exception as e:
                        print(f"  Prediction error: {e}, using default")
                        pred_value = 3.0
                else:
                    # Dummy prediction based on features
                    pred_value = 1.0 + features.get("log_views", 0) * 0.5
                
                # Prepare output
                output = {
                    "video_id": record.get("video_id", "unknown"),
                    "title": record.get("title", "Unknown Title")[:100],  # Limit length
                    "country": record.get("country", "Unknown"),
                    "predicted_trending_days": round(max(0.0, pred_value), 2),
                    "prediction_time": datetime.now().isoformat()
                }
                
                # Send to output topic
                producer.send(args.output_topic, value=output)
                
                if message_count % 10 == 0:
                    print(f"✓ Processed {message_count} messages. Last prediction: {output['video_id']} -> {output['predicted_trending_days']} days")
                
            except Exception as e:
                print(f"✗ Error processing message: {e}")
                continue
                
    except KeyboardInterrupt:
        print(f"\n✓ Streaming stopped by user after {message_count} messages")
    finally:
        consumer.close()
        producer.close()
        print("Resources cleaned up")

if __name__ == "__main__":
    run_stream()
