#!/usr/bin/env python3

import json
import argparse
import time
from kafka import KafkaProducer

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--kafka-servers", default="localhost:9092")
    parser.add_argument("--topic", default="youtube_videos")
    parser.add_argument("--count", type=int, default=5)
    args = parser.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=args.kafka_servers,
        value_serializer=lambda m: json.dumps(m).encode('utf-8')
    )

    # Sample video records
    sample_videos = [
        {
            "video_id": "dQw4w9WgXcQ",
            "title": "Rick Astley - Never Gonna Give You Up",
            "publish_time": "2009-10-25 00:00:00",
            "tags": "Music",
            "views": 1000000,
            "likes": 50000,
            "comment_count": 20000,
            "country": "US"
        },
        {
            "video_id": "9bZkp7q19f0",
            "title": "PSY - GANGNAM STYLE",
            "publish_time": "2012-07-15 00:00:00",
            "tags": "Music",
            "views": 3000000,
            "likes": 150000,
            "comment_count": 50000,
            "country": "KR"
        },
        {
            "video_id": "5NV6Rdv1a3I",
            "title": "Pinkfong Baby Shark Dance",
            "publish_time": "2016-06-18 00:00:00",
            "tags": "Children",
            "views": 2000000,
            "likes": 100000,
            "comment_count": 30000,
            "country": "KR"
        },
        {
            "video_id": "kfVsfOSbJY0",
            "title": "Eminem - Lose Yourself",
            "publish_time": "2002-10-28 00:00:00",
            "tags": "Music",
            "views": 800000,
            "likes": 40000,
            "comment_count": 15000,
            "country": "US"
        },
        {
            "video_id": "oHg5SJYRHA0",
            "title": "YouTube Rewind 2019",
            "publish_time": "2019-12-05 00:00:00",
            "tags": "Entertainment",
            "views": 500000,
            "likes": 10000,
            "comment_count": 50000,
            "country": "US"
        }
    ]

    print(f"Sending {args.count} sample videos to Kafka topic '{args.topic}'...")
    for i in range(args.count):
        video = sample_videos[i % len(sample_videos)]
        producer.send(args.topic, value=video)
        print(f"  ✓ Sent: {video['video_id']} - {video['title']}")
        time.sleep(1)

    producer.flush()
    producer.close()
    print(f"✓ Sent {args.count} messages to Kafka")

if __name__ == "__main__":
    main()
