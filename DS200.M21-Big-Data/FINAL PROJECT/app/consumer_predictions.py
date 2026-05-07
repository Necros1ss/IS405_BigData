#!/usr/bin/env python3
"""
Kafka consumer for YouTube trending predictions.
Reads real-time predictions from the streaming pipeline.

Usage:
    python3 -m app.consumer_predictions --kafka-servers localhost:9092 --topic youtube_predictions
"""
import argparse
import json
from datetime import datetime

def main():
    """Main consumer loop."""
    parser = argparse.ArgumentParser(description="Kafka consumer for trending predictions")
    parser.add_argument("--kafka-servers", default="localhost:9092", help="Kafka broker addresses")
    parser.add_argument("--topic", default="youtube_predictions", help="Kafka topic to consume from")
    parser.add_argument("--group", default="trending_consumer", help="Consumer group ID")
    
    args = parser.parse_args()
    
    try:
        from kafka import KafkaConsumer
    except ImportError:
        print("✗ kafka-python not installed. Install with: pip install kafka-python")
        return False
    
    try:
        consumer = KafkaConsumer(
            args.topic,
            bootstrap_servers=args.kafka_servers,
            group_id=args.group,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest',
            max_poll_records=1
        )
        print(f"✓ Connected to Kafka at {args.kafka_servers}")
    except Exception as e:
        print(f"✗ Failed to connect to Kafka: {e}")
        print("Tip: Make sure Kafka is running and topic exists")
        return False
    
    print(f"\n=== YouTube Trending Predictions Consumer ===")
    print(f"Topic: {args.topic}")
    print(f"Group: {args.group}")
    print(f"\nListening for predictions...\n")
    print("-" * 100)
    
    message_count = 0
    
    try:
        for message in consumer:
            message_count += 1
            prediction = message.value
            
            # Format output
            timestamp = datetime.now().strftime("%H:%M:%S")
            video_id = prediction.get("video_id", "unknown")
            title = prediction.get("title", "Unknown")
            publish_date = prediction.get("publish_date", "unknown")
            trending = prediction.get("trending", 0)
            prob_trending = prediction.get("prob_trending", 0)
            prob_not_trending = prediction.get("prob_not_trending", 0)
            
            # Color based on prediction
            status = "🔥 TRENDING" if trending == 1 else "❄️ NOT TRENDING"
            
            print(f"[{timestamp}] #{message_count:04d} {status}")
            print(f"  Video ID: {video_id}")
            print(f"  Title: {title}")
            print(f"  Publish Date: {publish_date}")
            print(f"  Probability: Not Trending {prob_not_trending:.2%} | Trending {prob_trending:.2%}")
            print("-" * 100)
    
    except KeyboardInterrupt:
        print(f"\n\n✓ Consumer stopped by user. Received {message_count} predictions.")
    finally:
        consumer.close()
    
    return True


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
