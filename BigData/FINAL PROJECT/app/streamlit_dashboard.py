#!/usr/bin/env python3
"""
Real-time Streamlit Dashboard - Reads from Kafka and displays streaming metrics
"""

import os
import json
from datetime import datetime
from collections import deque
import logging

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from kafka import KafkaConsumer
from kafka.structs import TopicPartition
from kafka.errors import KafkaError
from streamlit_autorefresh import st_autorefresh

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def safe_json_deserializer(raw_message):
    """Deserialize Kafka payload safely and skip malformed JSON records."""
    if not raw_message:
        return None

    try:
        return json.loads(raw_message.decode('utf-8'))
    except Exception as e:
        logger.warning(f"Skipping malformed Kafka message: {e}")
        return None

# ============================================================
# CONFIGURATION
# ============================================================
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
# Nghe topic chứa kết quả dự đoán của Spark ML
PREDICTIONS_TOPIC = os.getenv("PREDICTIONS_TOPIC", os.getenv("YOUTUBE_TOPIC", "youtube_predictions"))
STREAMLIT_GROUP_ID = os.getenv("STREAMLIT_GROUP_ID", "streamlit-dashboard-v3")
STREAMLIT_AUTO_OFFSET_RESET = os.getenv("STREAMLIT_AUTO_OFFSET_RESET", "earliest")
METRICS_PATH = os.getenv("METRICS_PATH", "metrics/regression_metrics.json")

# ============================================================
# PAGE CONFIG
# ============================================================
st.set_page_config(
    page_title="Real-time YouTube Trending Dashboard",
    page_icon="🔥",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
    <style>
    .metric-box {
        padding: 20px;
        border-radius: 10px;
        background-color: #f0f2f6;
        margin: 10px 0;
    }
    </style>
""", unsafe_allow_html=True)

# ============================================================
# SESSION STATE & KAFKA CONSUMER
# ============================================================
@st.cache_resource
def get_kafka_consumer():
    """Initialize Kafka Consumer with retry logic"""
    max_retries = 5
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=[KAFKA_BROKER],
                auto_offset_reset=STREAMLIT_AUTO_OFFSET_RESET,
                enable_auto_commit=True,
                value_deserializer=safe_json_deserializer,
                session_timeout_ms=30000,
                max_poll_records=100
            )
            partitions = consumer.partitions_for_topic(PREDICTIONS_TOPIC)
            if not partitions:
                raise RuntimeError(f"No partitions available for topic {PREDICTIONS_TOPIC}")

            topic_partitions = [TopicPartition(PREDICTIONS_TOPIC, partition) for partition in sorted(partitions)]
            consumer.assign(topic_partitions)
            if STREAMLIT_AUTO_OFFSET_RESET == "earliest":
                consumer.seek_to_beginning(*topic_partitions)

            logger.info(f"✅ Connected to Kafka broker: {KAFKA_BROKER}, Topic: {PREDICTIONS_TOPIC}")
            return consumer
        except Exception as e:
            retry_count += 1
            logger.warning(f"⚠️ Connection attempt {retry_count}/{max_retries} failed: {str(e)}")
            if retry_count < max_retries:
                import time
                time.sleep(2)
    
    logger.error(f"❌ Failed to connect to Kafka after {max_retries} attempts")
    return None


def initialize_session_state():
    """Initialize Streamlit session state"""
    if 'videos_data' not in st.session_state:
        st.session_state.videos_data = deque(maxlen=100)  # Keep last 100 videos
    
    if 'metrics_data' not in st.session_state:
        st.session_state.metrics_data = deque(maxlen=500)  # Keep last 500 data points
    
    if 'last_update' not in st.session_state:
        st.session_state.last_update = datetime.now()
    
    if 'message_count' not in st.session_state:
        st.session_state.message_count = 0
    
    if 'consumer_active' not in st.session_state:
        st.session_state.consumer_active = True

    if 'consumer_bootstrapped' not in st.session_state:
        st.session_state.consumer_bootstrapped = False

    # Cache file to persist messages across browser refreshes
    if 'cache_path' not in st.session_state:
        st.session_state.cache_path = os.path.join('metrics', 'streamlit_cache.json')

    # Load persisted cache if exists to survive F5
    if os.path.exists(st.session_state.cache_path) and not st.session_state.videos_data:
        try:
            with open(st.session_state.cache_path, 'r', encoding='utf-8') as fh:
                payload = json.load(fh)
                for item in payload.get('videos', []):
                    # Ensure timestamp objects converted back
                    if isinstance(item.get('timestamp'), str):
                        try:
                            item['timestamp'] = datetime.fromisoformat(item['timestamp'])
                            item['timestamp_str'] = item['timestamp'].strftime("%Y-%m-%d %H:%M:%S")
                        except Exception:
                            item['timestamp'] = datetime.now()
                    st.session_state.videos_data.append(item)
                for m in payload.get('metrics', []):
                    if isinstance(m.get('timestamp'), str):
                        try:
                            m['timestamp'] = datetime.fromisoformat(m['timestamp'])
                        except Exception:
                            m['timestamp'] = datetime.now()
                    st.session_state.metrics_data.append(m)
                st.session_state.message_count = payload.get('message_count', st.session_state.message_count)
        except Exception as exc:
            logger.warning("Unable to load streamlit cache: %s", exc)


def poll_kafka_messages(max_messages=150):
    """Poll Kafka messages in Streamlit runtime context."""
    consumer = get_kafka_consumer()

    if consumer is None:
        st.session_state.consumer_active = False
        return

    try:
        records = consumer.poll(timeout_ms=200, max_records=max_messages)
        record_count = sum(len(partition_records) for partition_records in records.values())
        if record_count:
            logger.info("Kafka poll fetched %s record(s)", record_count)

        for _, partition_records in records.items():
            for message in partition_records:
                if message.value is None:
                    continue

                data = message.value
                # Normalize prediction field names so UI logic can rely on `predicted_trending_days`
                raw_pred = data.get('predicted_trending_days', data.get('prediction', 0))
                try:
                    data['predicted_trending_days'] = float(raw_pred or 0)
                except Exception:
                    data['predicted_trending_days'] = 0.0

                logger.info(
                    "Parsed prediction for video_id=%s predicted_trending_days=%s",
                    data.get('video_id', 'Unknown'),
                    data['predicted_trending_days'],
                )

                # Add timestamp
                data['timestamp'] = datetime.now()
                data['timestamp_str'] = data['timestamp'].strftime("%Y-%m-%d %H:%M:%S")

                # Store video data
                st.session_state.videos_data.append(data)

                # Extract metrics for charting (THÊM predicted_trending_days VÀO ĐÂY)
                metrics_point = {
                    'timestamp': data['timestamp'],
                    'views': float(data.get('views', 0) or data.get('view_count', 0)),
                    'likes': float(data.get('likes', 0)),
                    'comments': float(data.get('comment_count', 0)),
                    'predicted_trending_days': float(data.get('predicted_trending_days', 0)),
                    'video_id': data.get('video_id', 'Unknown')
                }
                st.session_state.metrics_data.append(metrics_point)

                st.session_state.message_count += 1
                st.session_state.last_update = datetime.now()

                # Persist cache to disk so a browser refresh (F5) doesn't lose data
                try:
                    cache_payload = {
                        'videos': list(st.session_state.videos_data),
                        'metrics': [
                            {
                                **{k: (v.isoformat() if isinstance(v, datetime) else v) for k, v in m.items()}
                            } for m in list(st.session_state.metrics_data)
                        ],
                        'message_count': st.session_state.message_count
                    }

                    os.makedirs(os.path.dirname(st.session_state.cache_path), exist_ok=True)
                    with open(st.session_state.cache_path, 'w', encoding='utf-8') as fh:
                        json.dump(cache_payload, fh, default=str)
                except Exception as exc:
                    logger.debug("Unable to write streamlit cache: %s", exc)

        st.session_state.consumer_active = True

    except KafkaError as e:
        logger.error(f"❌ Kafka error: {str(e)}")
        st.session_state.consumer_active = False
    except Exception as e:
        logger.error(f"❌ Processing error: {str(e)}")
        st.session_state.consumer_active = False


# ============================================================
# VISUALIZATION FUNCTIONS
# ============================================================
def create_views_timeline():
    """Create real-time views timeline chart"""
    if not st.session_state.metrics_data:
        st.warning("⏳ Waiting for data from Kafka...")
        return

    df = pd.DataFrame(list(st.session_state.metrics_data))
    df = df.sort_values('timestamp')

    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=df['timestamp'],
        y=df['views'],
        mode='lines+markers',
        name='Views',
        line=dict(color='#1f77b4', width=2),
        fill='tozeroy',  # Đổi sang dạng Area chart cho đẹp
        marker=dict(size=6),
        hovertemplate='<b>Views:</b> %{y:,.0f}<br><b>Time:</b> %{x}<extra></extra>'
    ))

    fig.update_layout(
        title='📈 Real-time Views Timeline',
        xaxis_title='Time',
        yaxis_title='Number of Views',
        template='plotly_dark' if st.get_option("theme.base") == "dark" else 'plotly_white',
        hovermode='x unified',
        height=400
    )

    st.plotly_chart(fig, use_container_width=True)


def create_engagement_metrics():
    """Create engagement metrics comparison"""
    if not st.session_state.metrics_data:
        return

    df = pd.DataFrame(list(st.session_state.metrics_data))
    df = df.sort_values('timestamp')

    fig = go.Figure()

    # Normalize metrics to same scale for comparison
    max_views = df['views'].max() or 1
    max_likes = df['likes'].max() or 1
    max_comments = df['comments'].max() or 1

    fig.add_trace(go.Scatter(
        x=df['timestamp'],
        y=df['views'] / max_views * 100,
        mode='lines',
        name='Views (normalized)',
        line=dict(color='#1f77b4', width=2),
    ))

    fig.add_trace(go.Scatter(
        x=df['timestamp'],
        y=df['likes'] / max_likes * 100,
        mode='lines',
        name='Likes (normalized)',
        line=dict(color='#ff7f0e', width=2),
    ))

    fig.add_trace(go.Scatter(
        x=df['timestamp'],
        y=df['comments'] / max_comments * 100,
        mode='lines',
        name='Comments (normalized)',
        line=dict(color='#2ca02c', width=2),
    ))

    fig.update_layout(
        title='📊 Engagement Metrics (Normalized)',
        xaxis_title='Time',
        yaxis_title='Normalized Value (%)',
        template='plotly_dark' if st.get_option("theme.base") == "dark" else 'plotly_white',
        hovermode='x unified',
        height=400
    )

    st.plotly_chart(fig, use_container_width=True)


def create_metrics_distribution():
    """Create metrics distribution charts - Tích hợp Biểu đồ Dự đoán"""
    if not st.session_state.metrics_data:
        return

    df = pd.DataFrame(list(st.session_state.metrics_data))

    col1, col2 = st.columns(2)

    with col1:
        fig = px.histogram(df, x='views', nbins=20, title='Views Distribution')
        fig.update_layout(height=300)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        fig = px.histogram(df, x='likes', nbins=20, title='Likes Distribution')
        fig.update_layout(height=300)
        st.plotly_chart(fig, use_container_width=True)

    col3, col4 = st.columns(2)

    with col3:
        fig = px.histogram(df, x='comments', nbins=20, title='Comments Distribution')
        fig.update_layout(height=300)
        st.plotly_chart(fig, use_container_width=True)

    with col4:
        fig = px.histogram(
            df,
            x='predicted_trending_days',
            nbins=20,
            title='Trending-days Histogram',
            color_discrete_sequence=['#e76f51']
        )
        fig.update_layout(height=300)
        st.plotly_chart(fig, use_container_width=True)


@st.cache_data(ttl=30)
def load_training_metrics():
    """Load model comparison metrics saved by the training job."""
    if not os.path.exists(METRICS_PATH):
        return {}

    try:
        with open(METRICS_PATH, "r", encoding="utf-8") as handle:
            return json.load(handle)
    except Exception as exc:
        logger.warning("Unable to read training metrics from %s: %s", METRICS_PATH, exc)
        return {}


def display_model_insights(metrics):
    """Render the RMSE/MAE/R² panel and comparison table."""
    if not metrics:
        st.info("Model metrics are not available yet. Run the training job first.")
        return

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Best model", metrics.get("best_model", "N/A"))
    with col2:
        st.metric("RMSE", f"{float(metrics.get('rmse', 0.0)):.4f}")
    with col3:
        st.metric("MAE", f"{float(metrics.get('mae', 0.0)):.4f}")
    with col4:
        st.metric("R²", f"{float(metrics.get('r2', 0.0)):.4f}")

    st.subheader("Model comparison")
    comparison = pd.DataFrame(metrics.get("model_comparison", []))
    if not comparison.empty:
        st.dataframe(comparison.sort_values("rmse"), use_container_width=True)

    feature_importance = pd.DataFrame(metrics.get("feature_importance", []))
    if not feature_importance.empty:
        fig = px.bar(
            feature_importance.head(15),
            x="importance",
            y="feature",
            orientation="h",
            title="Feature importance",
            color="importance",
            color_continuous_scale="Viridis",
        )
        fig.update_layout(height=500, yaxis=dict(autorange="reversed"))
        st.plotly_chart(fig, use_container_width=True)


def create_statistics_table():
    """Create statistics summary table"""
    if not st.session_state.metrics_data:
        return

    df = pd.DataFrame(list(st.session_state.metrics_data))

    stats = {
        'Metric': ['Views', 'Likes', 'Comments', 'Predicted Days'],
        'Mean': [
            f"{df['views'].mean():,.0f}",
            f"{df['likes'].mean():,.0f}",
            f"{df['comments'].mean():,.0f}",
            f"{df['predicted_trending_days'].mean():,.1f}"
        ],
        'Max': [
            f"{df['views'].max():,.0f}",
            f"{df['likes'].max():,.0f}",
            f"{df['comments'].max():,.0f}",
            f"{df['predicted_trending_days'].max():,.1f}"
        ],
        'Min': [
            f"{df['views'].min():,.0f}",
            f"{df['likes'].min():,.0f}",
            f"{df['comments'].min():,.0f}",
            f"{df['predicted_trending_days'].min():,.1f}"
        ]
    }

    st.dataframe(pd.DataFrame(stats), use_container_width=True)


def display_latest_videos():
    """Display latest videos received - Tích hợp Emoji 🔥⚡❄️"""
    if not st.session_state.videos_data:
        st.info("⏳ No videos received yet")
        return

    videos_list = list(st.session_state.videos_data)
    videos_list.reverse()  # Show newest first

    max_show = int(st.session_state.get('max_videos_display', 10))
    for idx, video in enumerate(videos_list[:max_show], 1):
        col1, col2 = st.columns([3, 1])

        # Lấy số ngày dự đoán và gán mác Hotness
        days = float(video.get('predicted_trending_days', video.get('prediction', 0)))
        icon = "🔥 [KHỦNG]" if days >= 7.0 else ("⚡ [TRUNG BÌNH]" if days >= 2.0 else "❄️ [CHÌM]")

        with col1:
            st.write(f"**{idx}. {icon} {video.get('title', 'Unknown Title')}**")
            st.caption(f"ID: {video.get('video_id', 'N/A')} | Quốc gia: {video.get('country', 'N/A')} | {video.get('timestamp_str', 'N/A')}")

        with col2:
            st.metric("Dự đoán Trending", f"{days:.1f} ngày")
            st.caption(f"Views: {int(video.get('views', 0)):,}")


# ============================================================
# MAIN APPLICATION
# ============================================================
def main():
    initialize_session_state()

    # Re-run app periodically to keep charts live without manual refresh.
    st_autorefresh(interval=2000, key="kafka_autorefresh")

    # Header
    st.title("🎥 Real-time YouTube Trending Predictor")
    st.markdown("### Powered by Kafka Streaming & Apache Spark ML")

    # Sidebar
    with st.sidebar:
        st.header("⚙️ Configuration")

        col1, col2 = st.columns(2)
        with col1:
            st.metric("Kafka Broker", KAFKA_BROKER)
        with col2:
            st.metric("Topic", PREDICTIONS_TOPIC)

        st.divider()

        # Status
        if st.session_state.consumer_active:
            st.success(f"✅ Consumer Active")
        else:
            st.error(f"❌ Consumer Inactive")

        st.metric("Messages Received", st.session_state.message_count)
        st.metric("Last Update", st.session_state.last_update.strftime("%H:%M:%S"))

        st.divider()

        # Display controls
        st.subheader("Display")
        max_videos_display = st.slider("Max videos to show", min_value=5, max_value=50, value=10, step=1)
        st.session_state.max_videos_display = max_videos_display

        st.divider()

        # Start Consumer Button
        col1, col2 = st.columns(2)
        with col1:
            if st.button("🔄 Refresh Data", use_container_width=True):
                st.rerun()

        with col2:
            if st.button("🧹 Clear Data", use_container_width=True):
                st.session_state.videos_data.clear()
                st.session_state.metrics_data.clear()
                st.session_state.message_count = 0
                # Reset Kafka consumer offsets so the dashboard can re-read historical messages
                try:
                    consumer = get_kafka_consumer()
                    if consumer:
                        partitions = consumer.partitions_for_topic(PREDICTIONS_TOPIC)
                        if partitions:
                            topic_partitions = [TopicPartition(PREDICTIONS_TOPIC, p) for p in sorted(partitions)]
                            consumer.assign(topic_partitions)
                            consumer.seek_to_beginning(*topic_partitions)
                            logger.info("Consumer offsets reset to beginning for topic %s", PREDICTIONS_TOPIC)
                except Exception as e:
                    logger.warning("Unable to reset Kafka consumer offsets: %s", e)
                # Clear persistent cache
                try:
                    if os.path.exists(st.session_state.cache_path):
                        os.remove(st.session_state.cache_path)
                except Exception:
                    pass
                st.rerun()

        st.divider()
        st.info("""
        📡 **Hệ thống hoạt động:**
        1. Spark Structured Streaming nhận video từ Kafka.
        2. ML Pipeline dự đoán số ngày lọt Top Trending.
        3. Đẩy kết quả sang topic Prediction.
        4. Dashboard hứng và hiển thị Real-time.
        """)

    # Main content
    try:
        training_metrics = load_training_metrics()

        if st.session_state.consumer_active:
            poll_kafka_messages()

        # Key Metrics - Thêm cột thứ 5 cho AI Prediction
        if st.session_state.metrics_data:
            df_metrics = pd.DataFrame(list(st.session_state.metrics_data))

            col1, col2, col3, col4, col5 = st.columns(5)
            with col1:
                st.metric("📊 Avg Views", f"{df_metrics['views'].mean():,.0f}")
            with col2:
                st.metric("👍 Avg Likes", f"{df_metrics['likes'].mean():,.0f}")
            with col3:
                st.metric("💬 Avg Comments", f"{df_metrics['comments'].mean():,.0f}")
            with col4:
                # Cột hiển thị trung bình số ngày trend dự đoán
                st.metric("🎯 Avg Trend Days", f"{df_metrics['predicted_trending_days'].mean():,.1f}")
            with col5:
                st.metric("📈 Data Points", len(df_metrics))
        else:
            st.warning(f"⏳ Waiting for AI Predictions from topic '{PREDICTIONS_TOPIC}'...")

        st.divider()

        st.subheader("Model Performance")
        display_model_insights(training_metrics)

        st.divider()

        # Tabs for different views
        tab1, tab2, tab3, tab4 = st.tabs(["📈 Real-time Trends", "📋 Predict Trending Video", "📉 Phân bổ & Thống kê", "📊 Engagement Analysis"])

        with tab1:
            st.subheader("Real-time Views Timeline")
            create_views_timeline()

        with tab2:
            st.subheader("Bảng Xếp Hạng Video Mới Nhất")
            display_latest_videos()

        with tab3:
            st.subheader("Metrics Distribution")
            create_metrics_distribution()
            st.subheader("Statistics Summary")
            create_statistics_table()

        with tab4:
            st.subheader("Normalized Engagement Metrics")
            create_engagement_metrics()

    except Exception as e:
        st.error(f"❌ Error: {str(e)}")
        logger.error(f"Application error: {str(e)}", exc_info=True)


if __name__ == "__main__":
    main()