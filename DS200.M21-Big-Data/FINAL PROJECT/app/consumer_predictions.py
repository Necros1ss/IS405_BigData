#!/usr/bin/env python3
import argparse
import json
from datetime import datetime
from kafka import KafkaConsumer

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--kafka-servers", default="localhost:9092")
    parser.add_argument("--topic", default="youtube_predictions")
    args = parser.parse_args()
    
    consumer = KafkaConsumer(args.topic, bootstrap_servers=args.kafka_servers, value_deserializer=lambda m: json.loads(m.decode('utf-8')))
    print(f"\n=== LIVE PREDICTIONS ({args.topic}) ===")
    
    for idx, msg in enumerate(consumer, 1):
        p = msg.value
        days = p.get("predicted_trending_days", 0.0)
        icon = "🔥" if days >= 5.0 else ("⚡" if days >= 2.0 else "❄️")
        print(f"[{datetime.now().strftime('%H:%M:%S')}] #{idx:04d} {icon} Dự đoán trụ được: {days} NGÀY")
        print(f"  ID: {p.get('video_id', 'N/A')} | Lượt xem đầu: {p.get('views', 0):,.0f} | Tiêu đề: {p.get('title', 'N/A')}\n" + "-"*60)

if __name__ == "__main__":
    main()