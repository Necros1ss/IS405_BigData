#!/usr/bin/env python3
"""
Kafka producer for YouTube trending videos.
Simulates real-time video uploads and publishes them to Kafka.

Usage:
    python3 -m app.producer_youtube --kafka-servers localhost:9092 --topic youtube_videos --rate 1
"""
import argparse
import json
import time
import random
from datetime import datetime

def create_synthetic_video():
    """Generate a synthetic YouTube video record."""
    video_id = f"vid_{random.randint(100000, 999999)}"
    
    titles = [
        "Amazing Unboxing", "Gaming Highlights", "Music Video", "Tutorial", "Vlog",
        "Comedy Sketch", "News Update", "Sports Highlights", "Tech Review", "Cooking",
        "Travel Vlog", "DIY Project", "Educational", "Entertainment", "Reaction Video"
    ]
    
    tags = ["trending", "viral", "music", "educational", "entertainment", "gaming", 
            "comedy", "news", "sports", "tech", "tutorial", "food", "travel", "diy"]
    
    return {
        "video_id": video_id,
        "title": random.choice(titles) + f" #{random.randint(1, 1000)}",
        "view_count": random.randint(100, 1000000),
        "like_count": random.randint(10, 100000),
        "comment_count": random.randint(5, 50000),
        "video_tags": "|".join(random.sample(tags, random.randint(2, 6))),
        "description": f"Check out this awesome video! {random.choice(['Subscribe for more!', 'Like and comment!', 'Turn on notifications!'])}"
    }


def main():
    """Main producer loop."""
    parser = argparse.ArgumentParser(description="Kafka producer for YouTube videos")
    parser.add_argument("--kafka-servers", default="localhost:9092", help="Kafka broker addresses")
    parser.add_argument("--topic", default="youtube_videos", help="Kafka topic to publish to")
    parser.add_argument("--rate", type=float, default=1.0, 
        help="Publish rate (videos per second). Use 0.5 for 1 video every 2 seconds")
    parser.add_argument("--num-messages", type=int, default=None,
        help="Number of messages to send (None = infinite)")
    
    args = parser.parse_args()
    
    try:
        from kafka import KafkaProducer
    except ImportError:
        print("✗ kafka-python not installed. Install with: pip install kafka-python")
        return False
    
    try:
        producer = KafkaProducer(
            bootstrap_servers=args.kafka_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',
            retries=3
        )
        print(f"✓ Connected to Kafka at {args.kafka_servers}")
    except Exception as e:
        print(f"✗ Failed to connect to Kafka: {e}")
        print("Tip: Make sure Kafka is running: bin/kafka-server-start.sh config/server.properties")
        return False
    
    print(f"\n=== YouTube Video Producer ===")
    print(f"Topic: {args.topic}")
    print(f"Rate: {args.rate} videos/second")
    print(f"Messages: {'infinite' if args.num_messages is None else args.num_messages}")
    print(f"\nPress Ctrl+C to stop.\n")
    
    message_count = 0
    interval = 1.0 / args.rate if args.rate > 0 else 1.0
    
    try:
        while True:
            if args.num_messages and message_count >= args.num_messages:
                print(f"\n✓ Sent {message_count} messages. Stopping.")
                break
            
            video = create_synthetic_video()
            
            # Send to Kafka
            try:
                producer.send(args.topic, value=video)
                message_count += 1
                timestamp = datetime.now().strftime("%H:%M:%S")
                print(f"[{timestamp}] #{message_count:04d} Published: {video['video_id']} ({video['view_count']:,} views, {video['like_count']:,} likes)")
                
                # Wait before next message
                time.sleep(interval)
            except Exception as e:
                print(f"✗ Failed to send message: {e}")
    
    except KeyboardInterrupt:
        print(f"\n\n✓ Producer stopped by user. Sent {message_count} messages total.")
    finally:
        producer.close()
    
    return True


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
