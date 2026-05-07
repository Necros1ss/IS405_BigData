#!/usr/bin/env python3
"""
Kafka consumer for YouTube trending predictions (Regression).
Reads real-time predicted trending duration.
"""
import argparse
import json
from datetime import datetime

def main():
    parser = argparse.ArgumentParser(description="Kafka consumer for trending duration predictions")
    parser.add_argument("--kafka-servers", default="localhost:9092", help="Kafka broker addresses")
    parser.add_argument("--topic", default="youtube_predictions", help="Kafka topic to consume from")
    parser.add_argument("--group", default="trending_reg_consumer", help="Consumer group ID")
    args = parser.parse_args()
    
    try:
        from kafka import KafkaConsumer
        consumer = KafkaConsumer(
            args.topic,
            bootstrap_servers=args.kafka_servers,
            group_id=args.group,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest'
        )
    except ImportError:
        print("✗ kafka-python not installed.")
        return False
        
    print(f"\n=== YouTube Trending Duration Consumer ===")
    print(f"Listening for duration predictions on topic: {args.topic}\n")
    print("-" * 100)
    
    message_count = 0
    try:
        for message in consumer:
            message_count += 1
            pred = message.value
            
            timestamp = datetime.now().strftime("%H:%M:%S")
            video_id = pred.get("video_id", "unknown")
            title = pred.get("title", "Unknown")
            views = pred.get("views", 0)
            days = pred.get("predicted_trending_days", 0.0)
            
            # Color/Icon formatting based on duration
            if days >= 5.0:
                icon = "🔥 [KHỦNG]"
            elif days >= 2.0:
                icon = "⚡ [TRUNG BÌNH]"
            else:
                icon = "❄️ [CHÌM]"
            
            print(f"[{timestamp}] #{message_count:04d} {icon} Dự đoán trụ được: {days} NGÀY")
            print(f"  Video ID: {video_id} | Lượt xem ban đầu: {views:,.0f}")
            print(f"  Tiêu đề: {title}")
            print("-" * 100)
    except KeyboardInterrupt:
        print(f"\n✓ Consumer stopped. Received {message_count} predictions.")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()