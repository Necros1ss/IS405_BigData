#!/usr/bin/env python3
"""
Kafka producer for YouTube trending videos.
Simulates real-time video uploads and publishes them to Kafka.
(Updated schema for Regression Pipeline)
"""
import argparse
import json
import os
import time
import random
from datetime import datetime, timezone
import requests

def load_env_file(env_path: str = ".env"):
    if not os.path.isfile(env_path): return
    with open(env_path, "r", encoding="utf-8") as f:
        for line in f:
            stripped = line.strip()
            if not stripped or stripped.startswith("#") or "=" not in stripped: continue
            key, value = stripped.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = value

def create_synthetic_video(profile: str | None = None):
    video_id = f"vid_{random.randint(100000, 999999)}"
    titles = ["Amazing Unboxing", "Gaming Highlights", "Music Video", "Tutorial", "Vlog", "Comedy Sketch", "News Update", "Tech Review", "Cooking", "Travel Vlog", "DIY Project"]
    tags = ["trending", "viral", "music", "educational", "entertainment", "gaming", "comedy", "news", "sports", "tech", "tutorial"]
    
    if profile == "non_trending":
        title = random.choice(titles[:5]) + f" #{random.randint(1, 1000)}"
        description = ""
        video_tags = ""
        publish_date = datetime.now(timezone.utc).replace(hour=random.randint(0, 5), minute=0, second=0, microsecond=0)
        views = random.randint(100, 50000)
        likes = random.randint(10, 2000)
        dislikes = random.randint(0, 100)
        comment_count = random.randint(5, 500)
        language = random.choice(["Vietnamese", "Spanish", "Portuguese", "Japanese", "Korean"])
    else:
        title = random.choice(titles) + f" #{random.randint(1, 1000)}"
        description = f"Check out this awesome video! {random.choice(['Subscribe for more!', 'Like and comment!'])}"
        video_tags = "|".join(random.sample(tags, random.randint(2, 6)))
        publish_date = datetime.now(timezone.utc)
        views = random.randint(100, 1000000)
        likes = random.randint(10, 100000)
        dislikes = random.randint(10, 5000)
        comment_count = random.randint(5, 50000)
        language = "English"

    return {
        "video_id": video_id,
        "event_time": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "publish_date": publish_date.isoformat(timespec="seconds").replace("+00:00", "Z"),
        "language": language,
        "title": title,
        "views": float(views),
        "likes": float(likes),
        "dislikes": float(dislikes),
        "comment_count": float(comment_count),
        "video_tags": video_tags,
        "description": description,
        "comments_disabled": 0,
        "ratings_disabled": 0,
        "video_error_or_removed": 0
    }

def fetch_youtube_video_ids(api_key, query, region_code="US", max_results=10):
    url = "https://www.googleapis.com/youtube/v3/search"
    params = {"key": api_key, "part": "snippet", "type": "video", "q": query, "order": "date", "regionCode": region_code, "maxResults": max(1, min(int(max_results), 50))}
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    items = resp.json().get("items", [])
    return [item.get("id", {}).get("videoId") for item in items if item.get("id", {}).get("videoId")]

def fetch_youtube_video_details(api_key, video_ids):
    if not video_ids: return []
    url = "https://www.googleapis.com/youtube/v3/videos"
    params = {"key": api_key, "part": "snippet,statistics", "id": ",".join(video_ids[:50]), "maxResults": min(len(video_ids), 50)}
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    items = resp.json().get("items", [])

    results = []
    for item in items:
        snippet = item.get("snippet", {})
        stats = item.get("statistics", {})
        tags = snippet.get("tags") or []
        
        results.append({
            "video_id": item.get("id", ""),
            "event_time": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
            "publish_date": snippet.get("publishedAt", ""),
            "language": snippet.get("defaultAudioLanguage", snippet.get("defaultLanguage", "en")),
            "title": snippet.get("title", ""),
            "views": float(stats.get("viewCount", 0) or 0),
            "likes": float(stats.get("likeCount", 0) or 0),
            "dislikes": float(stats.get("dislikeCount", 0) or 0),
            "comment_count": float(stats.get("commentCount", 0) or 0),
            "video_tags": "|".join(tags if isinstance(tags, list) else []),
            "description": snippet.get("description", ""),
            "comments_disabled": 0,
            "ratings_disabled": 0,
            "video_error_or_removed": 0
        })
    return results

def main():
    parser = argparse.ArgumentParser(description="Kafka producer for YouTube videos")
    parser.add_argument("--kafka-servers", default="localhost:9092", help="Kafka broker addresses")
    parser.add_argument("--topic", default="youtube_videos", help="Kafka topic to publish to")
    parser.add_argument("--source", choices=["synthetic", "youtube-api"], default="synthetic")
    parser.add_argument("--rate", type=float, default=1.0)
    parser.add_argument("--poll-interval", type=float, default=60.0)
    parser.add_argument("--num-messages", type=int, default=None)
    parser.add_argument("--burst-size", type=int, default=1)
    parser.add_argument("--non-trending-ratio", type=float, default=0.5)
    parser.add_argument("--query", default="trending")
    parser.add_argument("--region-code", default="US")
    parser.add_argument("--max-results", type=int, default=10)
    
    args = parser.parse_args()
    load_env_file()

    youtube_api_key = os.environ.get("YOUTUBE_API_KEY", "")
    if args.source == "youtube-api" and not youtube_api_key:
        print("✗ YOUTUBE_API_KEY is not set.")
        return False
    
    from kafka import KafkaProducer
    producer = KafkaProducer(
        bootstrap_servers=args.kafka_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all', retries=3
    )
    
    print(f"\n=== YouTube Video Producer (Regression Schema) ===")
    print(f"Source: {args.source} | Topic: {args.topic}")
    
    message_count = 0
    interval = 1.0 / args.rate if args.rate > 0 else 1.0
    sent_video_ids = set()
    
    try:
        while True:
            if args.num_messages and message_count >= args.num_messages: break

            if args.source == "synthetic":
                for _ in range(max(1, int(args.burst_size))):
                    if args.num_messages and message_count >= args.num_messages: break
                    profile = "non_trending" if random.random() < args.non_trending_ratio else None
                    video = create_synthetic_video(profile=profile)
                    producer.send(args.topic, value=video)
                    message_count += 1
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] #{message_count:04d} Published: {video['video_id']} ({video['views']:,.0f} views)")
                producer.flush()
                time.sleep(interval)
            else:
                try:
                    ids = fetch_youtube_video_ids(youtube_api_key, query=args.query, region_code=args.region_code, max_results=args.max_results)
                    videos = fetch_youtube_video_details(youtube_api_key, ids)
                    for video in videos:
                        if video["video_id"] in sent_video_ids: continue
                        producer.send(args.topic, value=video)
                        sent_video_ids.add(video["video_id"])
                        message_count += 1
                        print(f"[{datetime.now().strftime('%H:%M:%S')}] #{message_count:04d} Published(API): {video['video_id']} ({video['views']:,.0f} views)")
                    time.sleep(max(args.poll_interval, 1.0))
                except Exception as e:
                    print(f"✗ API error: {e}")
                    time.sleep(5.0)
    except KeyboardInterrupt:
        print(f"\n✓ Producer stopped. Sent {message_count} messages.")
    finally:
        producer.close()
    return True

if __name__ == "__main__":
    main()