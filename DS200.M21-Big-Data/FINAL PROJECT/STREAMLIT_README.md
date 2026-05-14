# 🎬 Streamlit Real-time Dashboard for Kafka Streaming

A real-time visualization dashboard built with Streamlit that displays YouTube trending video metrics streamed through Apache Kafka.

## 🎯 Features

### Real-time Data Processing
- **Kafka Consumer** - Continuously reads YouTube video data from Kafka topic
- **Background Threading** - Non-blocking data collection with separate threads
- **Auto Retry** - Automatic reconnection with exponential backoff
- **Data Buffering** - Maintains last 100 videos and 500 data points for analysis

### Interactive Visualizations

#### 📈 Tab 1: Real-time Trends
- **Views Timeline Chart** - Line chart showing views over time with hover details
- Real-time updates as new data arrives
- Shows complete timeline of received videos

#### 📊 Tab 2: Engagement Analysis  
- **Normalized Metrics Comparison** - Views, likes, and comments on same scale
- Enables cross-metric trend analysis
- Identifies engagement patterns

#### 📉 Tab 3: Distribution Analysis
- **Views Distribution** - Histogram of view counts
- **Likes Distribution** - Distribution of likes across videos
- **Comments Distribution** - Distribution of comment counts
- **Statistics Summary** - Mean, max, min, standard deviation

#### 📋 Tab 4: Latest Videos
- Real-time feed of recently received videos
- Shows title, video ID, timestamp, and view count
- Latest 10 videos displayed

### Dashboard Sidebar
- **Connection Status** - Shows Kafka connection state
- **Message Counter** - Total messages received
- **Timestamp** - Last data update time
- **Quick Actions** - Refresh data and clear cache
- **Configuration Info** - Current Kafka settings

## 🚀 Quick Start

### Option 1: Docker (Recommended)

```bash
# Clone/navigate to project directory
cd "DS200.M21-Big-Data/FINAL PROJECT"

# Start all services
chmod +x start-docker.sh
./start-docker.sh

# Open browser
# Streamlit: http://localhost:8501
# Kafka UI: http://localhost:8080
```

### Option 2: Local Python

```bash
# Install dependencies
pip install -r requirements.txt
pip install -r requirements-streamlit.txt

# Start Kafka separately (ensure broker running on localhost:9092)
# Then run Streamlit
streamlit run app/streamlit_dashboard.py
```

## 📊 Data Flow

```
Producer (YouTube API)
         ↓
   Kafka Topic: youtube_videos
         ↓
Streamlit Dashboard (Consumer)
         ↓
  Real-time Visualizations
```

## 🔧 Configuration

### Environment Variables

```bash
# .env or export before running
KAFKA_BROKER=localhost:9092         # Kafka broker address
YOUTUBE_TOPIC=youtube_videos        # Kafka topic name
STREAMLIT_SERVER_PORT=8501          # Streamlit port
STREAMLIT_SERVER_ADDRESS=0.0.0.0    # Streamlit bind address
```

### Streamlit Configuration

Edit `~/.streamlit/config.toml` for advanced settings:

```toml
[theme]
primaryColor = "#FF6B6B"
backgroundColor = "#FFFFFF"
secondaryBackgroundColor = "#F0F2F6"
textColor = "#262730"

[client]
toolbarMode = "viewer"
```

## 📈 Data Processing

### Input Data Format (from Kafka)

```json
{
  "video_id": "abc123xyz",
  "title": "Video Title",
  "publish_time": "2024-01-15T10:30:00Z",
  "tags": "tag1, tag2, tag3",
  "views": 50000.0,
  "view_count": 50000.0,
  "likes": 2500.0,
  "comment_count": 150.0,
  "country": "US"
}
```

### Processed Metrics

Each video message generates:
- `views` - Total view count
- `likes` - Total likes count
- `comments` - Total comments count
- `timestamp` - Server timestamp of reception

## 🎨 Visualization Details

### Real-time Updates
- Dashboard polls Kafka every 1 second
- Charts update automatically as new data arrives
- Maintains 500-point rolling window for performance

### Performance Optimization
- Data stored in `deque` with max length (circular buffer)
- Efficient pandas operations for aggregations
- Plotly for client-side rendering (no server redraw on hover)
- Background threading prevents UI blocking

### Interactivity
- **Hover Details** - Show exact values on hover
- **Click Legend** - Toggle series visibility
- **Pan & Zoom** - Interactive chart exploration
- **Download** - Export charts as PNG

## 🔍 Monitoring

### Check Connection Status

```bash
# Test Kafka connectivity
docker-compose exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092

# List topics
docker-compose exec kafka kafka-topics --list --bootstrap-server kafka:29092

# View messages
docker-compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic youtube_videos \
  --from-beginning \
  --max-messages 5
```

### View Logs

```bash
# Streamlit logs
docker-compose logs -f streamlit

# Application errors
docker-compose logs streamlit | grep ERROR

# Full container logs
docker-compose logs -f
```

## 🛠️ Troubleshooting

### Dashboard not updating

**Problem**: Charts show "Waiting for data"

**Solutions**:
1. Check Kafka producer is running
2. Verify Kafka broker connectivity: `docker-compose exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092`
3. Check topic exists: `docker-compose exec kafka kafka-topics --list --bootstrap-server kafka:29092`
4. View Streamlit logs: `docker-compose logs streamlit`

### Connection timeout

**Problem**: "Connection failed after 5 retries"

**Solutions**:
1. Ensure Kafka is fully started: `docker-compose ps kafka`
2. Wait 30 seconds for Kafka initialization
3. Check KAFKA_BROKER env var: `echo $KAFKA_BROKER`
4. Restart Kafka: `docker-compose restart kafka`

### Memory issues

**Problem**: Dashboard becomes slow or crashes

**Solutions**:
1. Reduce data buffer size in code (lines 90-95)
2. Reduce Spark worker memory in docker-compose.yml
3. Clear dashboard cache: click "Clear Data" button
4. Restart dashboard: `docker-compose restart streamlit`

### Port conflicts

**Problem**: Port 8501 already in use

**Solutions**:
```bash
# Change port in docker-compose.yml
# Or kill existing process
lsof -i :8501 | grep -v PID | awk '{print $2}' | xargs kill -9
```

## 📚 Code Structure

```
app/
├── streamlit_dashboard.py    # Main dashboard app
├── config.py                 # Configuration
├── producer_youtube.py       # Kafka producer
└── ...

docker-compose.yml            # Container orchestration
docker-compose.override.yml   # Development settings
Dockerfile.streamlit          # Streamlit container
DOCKER_SETUP.md              # Docker guide
```

## 🔗 Related Services

### Kafka UI (Monitoring)
Access at: http://localhost:8080

- View all topics
- See message count and lag
- Inspect individual messages
- Monitor consumer groups

### Spark Master UI
Access at: http://localhost:8081

- View running jobs
- Monitor worker status
- Check memory usage
- View executor logs

## 📝 Advanced Usage

### Custom Metrics

Edit `streamlit_dashboard.py` to add custom calculations:

```python
# Add to metrics_point dictionary
metrics_point = {
    'timestamp': data['timestamp'],
    'views': float(data.get('views', 0)),
    'engagement_rate': likes / views if views > 0 else 0
}
```

### Export Data

```python
# In streamlit, use built-in CSV export
df = pd.DataFrame(list(st.session_state.metrics_data))
st.download_button(
    label="Download CSV",
    data=df.to_csv(index=False),
    file_name="metrics.csv"
)
```

## 🎓 Learning Resources

- **Streamlit Docs**: https://docs.streamlit.io/
- **Plotly Documentation**: https://plotly.com/python/
- **Kafka Python Client**: https://github.com/dpkp/kafka-python
- **Real-time Dashboards**: https://blog.streamlit.io/

## 📄 License

Same as parent project (FINAL PROJECT)

## ✅ Checklist

- [x] Docker setup for Kafka + Spark
- [x] Streamlit dashboard created
- [x] Real-time line charts implemented
- [x] Engagement metrics visualization
- [x] Distribution analysis
- [x] Error handling and retry logic
- [x] Performance optimization
- [x] Documentation and guides
- [ ] Unit tests (TODO)
- [ ] CI/CD integration (TODO)
