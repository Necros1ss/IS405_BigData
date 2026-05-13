#!/usr/bin/env python3
import argparse
import json
import logging
from datetime import datetime
from kafka import KafkaConsumer

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--kafka-servers", default="localhost:9092")
    parser.add_argument("--topic", default="youtube_predictions")
    args = parser.parse_args()
    
    try:
        consumer = KafkaConsumer(args.topic, bootstrap_servers=args.kafka_servers, 
                                 value_deserializer=lambda m: json.loads(m.decode('utf-8')))
    except Exception as e:
        logger.error(f"✗ Không kết nối được Kafka: {e}")
        return

    logger.info(f"\n========== 🔴 LIVE PREDICTIONS ({args.topic}) ==========")
    
    for idx, msg in enumerate(consumer, 1):
        try:
            p = msg.value
            days = p.get("predicted_trending_days", 0.0)
            views = p.get("views", p.get("view_count", 0.0))
            
            icon = "🔥 [KHỦNG]" if days >= 5.0 else ("⚡ [TRUNG BÌNH]" if days >= 2.0 else "❄️ [CHÌM]")
            
            logger.info(f"[{datetime.now().strftime('%H:%M:%S')}] #{idx:04d} {icon} Dự đoán trụ được: {days} NGÀY")
            logger.info(f"  ID: {p.get('video_id', 'N/A')} | Lượt xem đầu: {views:,.0f}")
            logger.info(f"  Tiêu đề: {p.get('title', 'N/A')}")
            logger.info("-" * 60)
        except Exception as e:
            logger.warning(f"⚠ Lỗi parse dữ liệu từ Kafka: {e}")

if __name__ == "__main__":
    main()