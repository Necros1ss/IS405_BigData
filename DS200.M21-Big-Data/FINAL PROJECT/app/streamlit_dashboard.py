#!/usr/bin/env python3
"""
Real-time Streamlit Dashboard - Reads from Kafka and displays streaming metrics
"""

import os
import json
import threading
import queue
from datetime import datetime, timedelta
from collections import deque
import logging

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from kafka import KafkaConsumer
from kafka.errors import KafkaError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
YOUTUBE_TOPIC = os.getenv("YOUTUBE_TOPIC", "youtube_videos")

# ============================================================
# PAGE CONFIG
# ============================================================
st.set_page_config(
    page_title="Real-time YouTube Trending Dashboard",
    page_icon="📊",
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
                YOUTUBE_TOPIC,
                bootstrap_servers=[KAFKA_BROKER],
                auto_offset_reset='latest',
                enable_auto_commit=True,
                group_id='streamlit-dashboard',
                value_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else None,
                consumer_timeout_ms=1000,
                session_timeout_ms=30000,
                max_poll_records=1
            )
            logger.info(f"✅ Connected to Kafka broker: {KAFKA_BROKER}, Topic: {YOUTUBE_TOPIC}")
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


def kafka_message_processor():
    """Process Kafka messages in a separate thread"""
    consumer = get_kafka_consumer()
    
    if consumer is None:
        st.session_state.consumer_active = False
        return
    
    try:
        for message in consumer:
            if message.value is None:
                continue
            
            data = message.value
            
            # Add timestamp
            data['timestamp'] = datetime.now()
            data['timestamp_str'] = data['timestamp'].strftime("%Y-%m-%d %H:%M:%S")
            
            # Store video data
            st.session_state.videos_data.append(data)
            
            # Extract metrics for charting
            metrics_point = {
                'timestamp': data['timestamp'],
                'views': float(data.get('views', 0) or data.get('view_count', 0)),
                'likes': float(data.get('likes', 0)),
                'comments': float(data.get('comment_count', 0)),
                'video_id': data.get('video_id', 'Unknown')
            }
            st.session_state.metrics_data.append(metrics_point)
            
            st.session_state.message_count += 1
            st.session_state.last_update = datetime.now()
            
            logger.info(f"📥 Received message #{st.session_state.message_count}: {data.get('title', 'N/A')}")
    
    except KafkaError as e:
        logger.error(f"❌ Kafka error: {str(e)}")
        st.session_state.consumer_active = False
    except Exception as e:
        logger.error(f"❌ Processing error: {str(e)}")


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
        marker=dict(size=6),
        hovertemplate='<b>Views:</b> %{y:,.0f}<br><b>Time:</b> %{x}<extra></extra>'
    ))
    
    fig.update_layout(
        title='📈 Real-time Views Timeline',
        xaxis_title='Time',
        yaxis_title='Number of Views',
        template='plotly_white',
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
        template='plotly_white',
        hovermode='x unified',
        height=400
    )
    
    st.plotly_chart(fig, use_container_width=True)


def create_metrics_distribution():
    """Create metrics distribution charts"""
    if not st.session_state.metrics_data:
        return
    
    df = pd.DataFrame(list(st.session_state.metrics_data))
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        fig = px.histogram(df, x='views', nbins=20, title='Views Distribution')
        fig.update_layout(height=300)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        fig = px.histogram(df, x='likes', nbins=20, title='Likes Distribution')
        fig.update_layout(height=300)
        st.plotly_chart(fig, use_container_width=True)
    
    with col3:
        fig = px.histogram(df, x='comments', nbins=20, title='Comments Distribution')
        fig.update_layout(height=300)
        st.plotly_chart(fig, use_container_width=True)


def create_statistics_table():
    """Create statistics summary table"""
    if not st.session_state.metrics_data:
        return
    
    df = pd.DataFrame(list(st.session_state.metrics_data))
    
    stats = {
        'Metric': ['Views', 'Likes', 'Comments'],
        'Mean': [
            f"{df['views'].mean():,.0f}",
            f"{df['likes'].mean():,.0f}",
            f"{df['comments'].mean():,.0f}"
        ],
        'Max': [
            f"{df['views'].max():,.0f}",
            f"{df['likes'].max():,.0f}",
            f"{df['comments'].max():,.0f}"
        ],
        'Min': [
            f"{df['views'].min():,.0f}",
            f"{df['likes'].min():,.0f}",
            f"{df['comments'].min():,.0f}"
        ],
        'Std Dev': [
            f"{df['views'].std():,.0f}",
            f"{df['likes'].std():,.0f}",
            f"{df['comments'].std():,.0f}"
        ]
    }
    
    st.dataframe(pd.DataFrame(stats), use_container_width=True)


def display_latest_videos():
    """Display latest videos received"""
    if not st.session_state.videos_data:
        st.info("⏳ No videos received yet")
        return
    
    videos_list = list(st.session_state.videos_data)
    videos_list.reverse()  # Show newest first
    
    for idx, video in enumerate(videos_list[:10], 1):
        col1, col2 = st.columns([3, 1])
        
        with col1:
            st.write(f"**{idx}. {video.get('title', 'N/A')}**")
            st.caption(f"ID: {video.get('video_id', 'N/A')} | {video.get('timestamp_str', 'N/A')}")
        
        with col2:
            st.metric("Views", f"{int(video.get('views', 0) or video.get('view_count', 0)):,}")


# ============================================================
# MAIN APPLICATION
# ============================================================
def main():
    initialize_session_state()
    
    # Header
    st.title("🎥 Real-time YouTube Trending Dashboard")
    st.markdown("### Powered by Kafka Streaming & Apache Spark")
    
    # Sidebar
    with st.sidebar:
        st.header("⚙️ Configuration")
        
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Kafka Broker", KAFKA_BROKER.split(':')[0])
        with col2:
            st.metric("Topic", YOUTUBE_TOPIC)
        
        st.divider()
        
        # Status
        if st.session_state.consumer_active:
            st.success(f"✅ Consumer Active")
        else:
            st.error(f"❌ Consumer Inactive")
        
        st.metric("Messages Received", st.session_state.message_count)
        st.metric("Last Update", st.session_state.last_update.strftime("%H:%M:%S"))
        
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
                st.rerun()
        
        st.divider()
        st.info("""
        📡 **How it works:**
        1. Producer sends YouTube data to Kafka
        2. Dashboard subscribes to topic
        3. Real-time visualization updates
        """)
    
    # Main content
    try:
        # Start Kafka consumer in background thread
        if st.session_state.consumer_active:
            threading.Thread(target=kafka_message_processor, daemon=True).start()
        
        # Key Metrics
        if st.session_state.metrics_data:
            df_metrics = pd.DataFrame(list(st.session_state.metrics_data))
            
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("📊 Avg Views", f"{df_metrics['views'].mean():,.0f}")
            with col2:
                st.metric("👍 Avg Likes", f"{df_metrics['likes'].mean():,.0f}")
            with col3:
                st.metric("💬 Avg Comments", f"{df_metrics['comments'].mean():,.0f}")
            with col4:
                st.metric("📈 Data Points", len(df_metrics))
        else:
            st.warning("⏳ Waiting for data from Kafka topic...")
        
        st.divider()
        
        # Tabs for different views
        tab1, tab2, tab3, tab4 = st.tabs(["📈 Real-time Trends", "📊 Engagement Analysis", "📉 Distribution", "📋 Latest Videos"])
        
        with tab1:
            st.subheader("Real-time Views Timeline")
            create_views_timeline()
        
        with tab2:
            st.subheader("Normalized Engagement Metrics")
            create_engagement_metrics()
        
        with tab3:
            st.subheader("Metrics Distribution")
            create_metrics_distribution()
            st.subheader("Statistics Summary")
            create_statistics_table()
        
        with tab4:
            st.subheader("Latest Videos Received")
            display_latest_videos()
    
    except Exception as e:
        st.error(f"❌ Error: {str(e)}")
        logger.error(f"Application error: {str(e)}", exc_info=True)


if __name__ == "__main__":
    main()
