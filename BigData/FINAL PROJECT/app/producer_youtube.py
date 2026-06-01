#!/usr/bin/env python3
"""
Kafka producer for REAL YouTube Trending videos.
Fetches videos directly from YouTube Trending tab (mostPopular)
and publishes them to Kafka.
"""

import logging
import argparse
import json
import os
import time
from datetime import datetime
from pathlib import Path

import requests
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    from app.config import (
        KAFKA_BROKER,
        YOUTUBE_TOPIC,
        YOUTUBE_API_KEY
    )
except ImportError:
    from config import (
        KAFKA_BROKER,
        YOUTUBE_TOPIC,
        YOUTUBE_API_KEY
    )


logger = logging.getLogger(__name__)


# =========================================================
# LOAD ENV
# =========================================================
def load_env_file(env_path: str = ".env"):
    project_root = Path(__file__).resolve().parents[1]
    candidates = [
        Path(env_path),
        project_root / ".env",
        project_root.parent / ".env",
        project_root / ".venv_spark" / "bin" / "activate",
    ]

    for path in candidates:
        if not path.is_file():
            continue

        with path.open("r", encoding="utf-8") as f:
            for line in f:
                stripped = line.strip()

                if not stripped or stripped.startswith("#"):
                    continue

                if stripped.startswith("export "):
                    stripped = stripped[len("export "):]

                if "=" not in stripped:
                    continue

                key, value = stripped.split("=", 1)

                key = key.strip()
                value = value.strip().strip('"').strip("'")

                if key and key not in os.environ:
                    os.environ[key] = value


def create_kafka_producer(kafka_servers: str, wait_timeout: int = 120, wait_interval: int = 5):
    """Create a Kafka producer, waiting for the broker to become available."""

    deadline = None if wait_timeout <= 0 else time.monotonic() + wait_timeout
    attempt = 0

    while True:
        attempt += 1
        try:
            return KafkaProducer(
                bootstrap_servers=kafka_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=5,
                linger_ms=100,
                compression_type="gzip",
                retry_backoff_ms=1000,
            )
        except NoBrokersAvailable as exc:
            if deadline is not None and time.monotonic() >= deadline:
                raise RuntimeError(
                    f"Kafka broker not available at {kafka_servers} after waiting {wait_timeout}s. "
                    "Start Kafka first, then rerun the producer."
                ) from exc

            remaining = "indefinitely" if deadline is None else f"{int(max(deadline - time.monotonic(), 0))}s"
            logger.warning(
                "Kafka broker not available at %s (attempt %s); retrying in %ss, remaining wait: %s",
                kafka_servers,
                attempt,
                wait_interval,
                remaining,
            )
            time.sleep(max(wait_interval, 1))


# =========================================================
# FETCH TRENDING VIDEOS
# =========================================================
def fetch_trending_videos(
    api_key,
    region_code="US",
    max_results=10,
    category_id=None
):
    """
    Fetch REAL trending videos from YouTube Trending tab.

    Supports paginated requests so callers can request more than the API
    single-request limit of 50 items. The function will page through
    results using `nextPageToken` and return up to `max_results` unique videos.
    A safe cap is applied (200) to avoid runaway requests.
    """

    url = "https://www.googleapis.com/youtube/v3/videos"
    max_results = int(max_results or 0)
    if max_results <= 0:
        max_results = 10

    MAX_CAP = 200
    target = min(max_results, MAX_CAP)

    session = requests.Session()
    retry = Retry(total=3, backoff_factor=1.5, status_forcelist=[429, 500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.mount("http://", HTTPAdapter(max_retries=retry))

    results = []
    seen = set()
    page_token = None

    while len(results) < target:
        per_request = min(50, target - len(results))
        params = {
            "key": api_key,
            "part": "snippet,statistics",
            "chart": "mostPopular",
            "regionCode": region_code,
            "maxResults": per_request,
        }
        if category_id:
            params["videoCategoryId"] = str(category_id)
        if page_token:
            params["pageToken"] = page_token

        resp = session.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        items = data.get("items", [])

        if not items:
            break

        for item in items:
            vid = item.get("id", "")
            if not vid or vid in seen:
                continue

            snippet = item.get("snippet", {})
            stats = item.get("statistics", {})

            view_count_raw = stats.get("viewCount")
            likes_raw = stats.get("likeCount")
            comments_raw = stats.get("commentCount")

            if view_count_raw is None or likes_raw is None or comments_raw is None:
                logging.info(
                    "Skipping %s: statistics unavailable (views=%s, likes=%s, comments=%s)",
                    vid,
                    view_count_raw,
                    likes_raw,
                    comments_raw,
                )
                continue

            tags = snippet.get("tags") or []

            video_data = {
                "video_id": vid,
                "title": snippet.get("title", ""),
                "description": snippet.get("description", ""),
                "publish_time": snippet.get("publishedAt", ""),
                "tags": "|".join(tags if isinstance(tags, list) else []),
                "views": float(view_count_raw),
                "view_count": float(view_count_raw),
                "likes": float(likes_raw),
                "comment_count": float(comments_raw),
                "country": region_code,
            }

            results.append(video_data)
            seen.add(vid)

            if len(results) >= target:
                break

        page_token = data.get("nextPageToken")
        if not page_token:
            break

    return results


# =========================================================
# MAIN
# =========================================================
def main():
    parser = argparse.ArgumentParser(
        description="Kafka Producer for YouTube Trending Videos"
    )

    parser.add_argument(
        "--kafka-servers",
        default=KAFKA_BROKER,
        help="Kafka broker addresses"
    )

    parser.add_argument(
        "--topic",
        default=YOUTUBE_TOPIC,
        help="Kafka topic"
    )

    parser.add_argument(
        "--region-code",
        default="VN",
        help="Trending region (US, VN, JP, KR, etc.)"
    )

    parser.add_argument(
        "--max-results",
        type=int,
        default=100,
        help="Max videos per poll (1-200)"
    )

    parser.add_argument(
        "--poll-interval",
        type=float,
        default=300.0,
        help="Polling interval in seconds"
    )

    parser.add_argument(
        "--rate",
        type=float,
        default=None,
        help="Messages per second alias for quick run (overrides --poll-interval as 1/rate)"
    )

    parser.add_argument(
        "--num-messages",
        type=int,
        default=None,
        help="Stop after sending N messages"
    )

    parser.add_argument(
        "--category-id",
        default=None,
        help="""
        Optional YouTube category:
            10 = Music
            20 = Gaming
            24 = Entertainment
            25 = News
        """
    )

    parser.add_argument(
        "--kafka-wait-timeout",
        type=int,
        default=120,
        help="Seconds to wait for Kafka to become available before failing (0 = wait forever)",
    )

    parser.add_argument(
        "--kafka-wait-interval",
        type=int,
        default=5,
        help="Seconds to wait between Kafka availability checks",
    )

    args = parser.parse_args()

    # Load .env
    load_env_file()

    youtube_api_key = os.getenv("YOUTUBE_API_KEY", YOUTUBE_API_KEY)

    if args.rate is not None and args.rate > 0:
        args.poll_interval = 1.0 / args.rate

    if not youtube_api_key:
        print("✗ YOUTUBE_API_KEY is not set.")
        return False

    # =====================================================
    # KAFKA PRODUCER
    # =====================================================
    producer = create_kafka_producer(
        args.kafka_servers,
        wait_timeout=args.kafka_wait_timeout,
        wait_interval=args.kafka_wait_interval,
    )

    print("\n========================================")
    print(" YouTube Trending Kafka Producer")
    print("========================================")

    print(f"Kafka Servers : {args.kafka_servers}")
    print(f"Topic         : {args.topic}")
    print("Source        : api")
    print(f"Region        : {args.region_code}")
    print(f"Max Results   : {args.max_results}")
    print(f"Poll Interval : {args.poll_interval}s")
    print(f"Kafka Wait    : {args.kafka_wait_timeout}s")

    if args.category_id:
        print(f"Category ID   : {args.category_id}")

    print("========================================\n")

    message_count = 0

    # Prevent duplicate sending
    sent_video_ids = set()

    try:
        while True:

            if (
                args.num_messages
                and message_count >= args.num_messages
            ):
                break

            try:
                videos = fetch_trending_videos(
                    api_key=youtube_api_key,
                    region_code=args.region_code,
                    max_results=args.max_results,
                    category_id=args.category_id
                )

                print(
                    f"\n[{datetime.now().strftime('%H:%M:%S')}] "
                    f"Fetched {len(videos)} trending videos"
                )

                for video in videos:

                    video_id = video["video_id"]

                    # Skip duplicates
                    if video_id in sent_video_ids:
                        continue

                    # Send to Kafka
                    producer.send(
                        args.topic,

                        key=video_id.encode("utf-8"),

                        value=video
                    )

                    sent_video_ids.add(video_id)

                    message_count += 1

                    print(
                        f"#{message_count:04d} "
                        f"Published: {video_id} | "
                        f"{video['view_count']:,.0f} views | "
                        f"{video['title'][:60]}"
                    )

                    if (
                        args.num_messages
                        and message_count >= args.num_messages
                    ):
                        break

                producer.flush()

                print(
                    f"✓ Total sent: {message_count}"
                )

                # Wait before next polling
                time.sleep(
                    max(args.poll_interval, 1.0)
                )

            except Exception as e:
                logger.exception("Producer error occurred: %s", e)

                time.sleep(5)

    except KeyboardInterrupt:
        print("\n========================================")
        print(" Producer stopped by user")
        print(f" Total messages sent: {message_count}")
        print("========================================")

    finally:
        producer.close()

    return True


# =========================================================
# ENTRY POINT
# =========================================================
if __name__ == "__main__":
    main()