#!/usr/bin/env python3
"""
Kafka producer for YouTube trending videos.
Simulates real-time video uploads and publishes them to Kafka.

Usage:
    python3 -m app.producer_youtube --kafka-servers localhost:9092 --topic youtube_videos --rate 1
    python3 -m app.producer_youtube --source youtube-api --query "music" --poll-interval 60
"""
import argparse
import json
import os
import time
import random
from datetime import datetime, timezone

import requests


def load_env_file(env_path: str = ".env"):
    """Load KEY=VALUE pairs from a local .env file into environment variables."""
    if not os.path.isfile(env_path):
        return

    with open(env_path, "r", encoding="utf-8") as f:
        for line in f:
            stripped = line.strip()
            if not stripped or stripped.startswith("#") or "=" not in stripped:
                continue
            key, value = stripped.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = value

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
        "event_time": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "title": random.choice(titles) + f" #{random.randint(1, 1000)}",
        "view_count": random.randint(100, 1000000),
        "like_count": random.randint(10, 100000),
        "comment_count": random.randint(5, 50000),
        "video_tags": "|".join(random.sample(tags, random.randint(2, 6))),
        "description": f"Check out this awesome video! {random.choice(['Subscribe for more!', 'Like and comment!', 'Turn on notifications!'])}"
    }


def fetch_youtube_video_ids(api_key, query, region_code="US", max_results=10):
    """Fetch candidate video IDs using YouTube search.list."""
    url = "https://www.googleapis.com/youtube/v3/search"
    params = {
        "key": api_key,
        "part": "snippet",
        "type": "video",
        "q": query,
        "order": "date",
        "regionCode": region_code,
        "maxResults": max(1, min(int(max_results), 50)),
    }
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    payload = resp.json()
    items = payload.get("items", [])

    ids = []
    for item in items:
        video_id = item.get("id", {}).get("videoId")
        if video_id:
            ids.append(video_id)
    return ids


def fetch_youtube_video_details(api_key, video_ids):
    """Fetch detailed metadata via YouTube videos.list and map to Kafka schema."""
    if not video_ids:
        return []

    url = "https://www.googleapis.com/youtube/v3/videos"
    params = {
        "key": api_key,
        "part": "snippet,statistics",
        "id": ",".join(video_ids[:50]),
        "maxResults": min(len(video_ids), 50),
    }
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    payload = resp.json()
    items = payload.get("items", [])

    results = []
    for item in items:
        snippet = item.get("snippet", {})
        stats = item.get("statistics", {})
        tags = snippet.get("tags") or []
        if not isinstance(tags, list):
            tags = []

        results.append({
            "video_id": item.get("id", ""),
            "event_time": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
            "title": snippet.get("title", ""),
            "view_count": int(stats.get("viewCount", 0) or 0),
            "like_count": int(stats.get("likeCount", 0) or 0),
            "comment_count": int(stats.get("commentCount", 0) or 0),
            "video_tags": "|".join(tags),
            "description": snippet.get("description", ""),
        })

    return results


def main():
    """Main producer loop."""
    parser = argparse.ArgumentParser(description="Kafka producer for YouTube videos")
    parser.add_argument("--kafka-servers", default="localhost:9092", help="Kafka broker addresses")
    parser.add_argument("--topic", default="youtube_videos", help="Kafka topic to publish to")
    parser.add_argument("--source", choices=["synthetic", "youtube-api"], default="synthetic",
        help="Data source mode: synthetic generator or YouTube Data API")
    parser.add_argument("--rate", type=float, default=1.0, 
        help="Publish rate (videos per second). Use 0.5 for 1 video every 2 seconds")
    parser.add_argument("--poll-interval", type=float, default=60.0,
        help="Polling interval in seconds for YouTube API mode")
    parser.add_argument("--num-messages", type=int, default=None,
        help="Number of messages to send (None = infinite)")
    parser.add_argument("--query", default="trending",
        help="Search query for YouTube API mode")
    parser.add_argument("--region-code", default="US",
        help="Region code for YouTube API search (e.g., US, VN)")
    parser.add_argument("--max-results", type=int, default=10,
        help="Max videos fetched per API poll (1-50)")
    
    args = parser.parse_args()

    load_env_file()

    youtube_api_key = os.environ.get("YOUTUBE_API_KEY", "")
    if args.source == "youtube-api" and not youtube_api_key:
        print("✗ YOUTUBE_API_KEY is not set. Put it in .env or export it in your shell.")
        return False
    
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
    print(f"Source: {args.source}")
    print(f"Topic: {args.topic}")
    if args.source == "synthetic":
        print(f"Rate: {args.rate} videos/second")
    else:
        print(f"Polling every: {args.poll_interval} seconds")
        print(f"Query: {args.query} | Region: {args.region_code} | Max results: {args.max_results}")
    print(f"Messages: {'infinite' if args.num_messages is None else args.num_messages}")
    print(f"\nPress Ctrl+C to stop.\n")
    
    message_count = 0
    interval = 1.0 / args.rate if args.rate > 0 else 1.0
    sent_video_ids = set()
    
    try:
        while True:
            if args.num_messages and message_count >= args.num_messages:
                print(f"\n✓ Sent {message_count} messages. Stopping.")
                break

            if args.source == "synthetic":
                video = create_synthetic_video()

                try:
                    producer.send(args.topic, value=video)
                    message_count += 1
                    timestamp = datetime.now().strftime("%H:%M:%S")
                    print(f"[{timestamp}] #{message_count:04d} Published: {video['video_id']} ({video['view_count']:,} views, {video['like_count']:,} likes)")
                    time.sleep(interval)
                except Exception as e:
                    print(f"✗ Failed to send message: {e}")
            else:
                try:
                    ids = fetch_youtube_video_ids(
                        youtube_api_key,
                        query=args.query,
                        region_code=args.region_code,
                        max_results=args.max_results,
                    )
                    videos = fetch_youtube_video_details(youtube_api_key, ids)

                    published_in_this_poll = 0
                    for video in videos:
                        if not video.get("video_id") or video["video_id"] in sent_video_ids:
                            continue

                        producer.send(args.topic, value=video)
                        sent_video_ids.add(video["video_id"])
                        message_count += 1
                        published_in_this_poll += 1
                        timestamp = datetime.now().strftime("%H:%M:%S")
                        print(f"[{timestamp}] #{message_count:04d} Published(API): {video['video_id']} ({video['view_count']:,} views, {video['like_count']:,} likes)")

                        if args.num_messages and message_count >= args.num_messages:
                            break

                    if published_in_this_poll == 0:
                        timestamp = datetime.now().strftime("%H:%M:%S")
                        print(f"[{timestamp}] No new videos from API in this poll.")

                    if args.num_messages and message_count >= args.num_messages:
                        print(f"\n✓ Sent {message_count} messages. Stopping.")
                        break

                    time.sleep(max(args.poll_interval, 1.0))
                except requests.HTTPError as e:
                    print(f"✗ YouTube API HTTP error: {e}")
                    time.sleep(max(args.poll_interval, 5.0))
                except requests.RequestException as e:
                    print(f"✗ YouTube API network error: {e}")
                    time.sleep(max(args.poll_interval, 5.0))
                except Exception as e:
                    print(f"✗ API mode failure: {e}")
                    time.sleep(max(args.poll_interval, 5.0))
    
    except KeyboardInterrupt:
        print(f"\n\n✓ Producer stopped by user. Sent {message_count} messages total.")
    finally:
        producer.close()
    
    return True


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
