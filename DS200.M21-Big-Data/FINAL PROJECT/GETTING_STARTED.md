# 🎬 Getting Started with Real-time Kafka + Streamlit Dashboard

## 🚀 5-Minute Quick Start

### Step 1: Verify Setup (1 min)
```bash
cd "FINAL PROJECT"
chmod +x verify-setup.sh
./verify-setup.sh
```

All checks should show ✓ (green checkmarks)

### Step 2: Start Docker Services (2 min)
```bash
./start-docker.sh
```

Wait for message:
```
✅ Platform started successfully!
```

### Step 3: Start Kafka Producer (1 min)
```bash
# In a new terminal
python app/producer_youtube.py --loop --interval 10
```

You should see:
```
Connected to Kafka broker...
Publishing messages...
```

### Step 4: Open Dashboard (1 min)
Open browser: **http://localhost:8501**

You should see the Streamlit dashboard loading with real-time charts updating!

---

## 📊 Dashboard Walkthrough

### 🎯 Main Metrics (Top)
- **Avg Views**: Average views across all videos
- **Avg Likes**: Average likes across all videos
- **Avg Comments**: Average comments per video
- **Data Points**: Number of metrics collected

### 📈 Tab 1: Real-time Trends
- **Line Chart** showing views over time
- Updates every second as data arrives
- Hover to see exact values
- Zoom and pan with mouse

**What to look for:**
- Views should steadily increase
- Should see 10+ data points after 2 minutes

### 📊 Tab 2: Engagement Analysis
- **Multi-line chart** with normalized metrics
- Views, likes, and comments on same scale
- Identify engagement ratios

**What to look for:**
- All three lines should show trends
- Easier to compare which metric is higher

### 📉 Tab 3: Distribution Analysis
- **Histograms** showing data distribution
- **Statistics table** with mean, max, min, std dev

**What to look for:**
- Histograms should show bell curve shape
- Statistics updated in real-time

### 📋 Tab 4: Latest Videos
- **List of newest videos** received
- Shows title, ID, timestamp, views

**What to look for:**
- Should see videos appearing in real-time
- Timestamps should be recent

---

## 🔌 Monitoring and Control

### Kafka Monitoring
Open: **http://localhost:8080** (Kafka UI)

**What you can do:**
- View `youtube_videos` topic
- See message count
- Inspect individual messages
- Monitor consumer lag

### Spark Monitoring
Open: **http://localhost:8081** (Spark Master)

**What you can do:**
- View running jobs
- Check worker status
- Monitor memory usage
- View executor logs

### Streamlit Controls (Left Sidebar)
- **Connection Status**: Shows if Kafka is connected
- **Messages Received**: Total message count
- **Last Update**: Timestamp of last data
- **Refresh Data**: Force update charts
- **Clear Data**: Reset dashboard cache

---

## 🎯 Common Scenarios

### Scenario 1: Dashboard shows "Waiting for data"
**Problem**: No data appearing in charts

**Solution**:
1. Check producer is running:
   ```bash
   # Look for "Publishing messages..." in producer terminal
   ```
2. Check Kafka is healthy:
   ```bash
   docker-compose logs kafka | tail -10
   ```
3. If not, restart Kafka:
   ```bash
   docker-compose restart kafka
   sleep 10
   ```

### Scenario 2: Producer can't connect to Kafka
**Problem**: Producer says "Connection refused"

**Solution**:
1. Check Kafka is fully started (takes ~30 seconds):
   ```bash
   docker-compose ps
   # kafka should show healthy status
   ```
2. Wait longer and retry:
   ```bash
   sleep 30
   python app/producer_youtube.py --loop --interval 10
   ```

### Scenario 3: Streamlit keeps refreshing
**Problem**: Dashboard refreshes constantly

**Solution**:
1. This is normal - Streamlit app reruns on data change
2. If too frequent, check producer interval:
   ```bash
   # Change --interval to larger value (default 10 seconds)
   python app/producer_youtube.py --loop --interval 30
   ```

### Scenario 4: Port already in use
**Problem**: "Port 8501 already in use"

**Solution**:
```bash
# Find what's using the port
lsof -i :8501

# Kill the process if needed
kill -9 <PID>

# Or change port in docker-compose.yml
```

---

## 📊 Expected Output

### After 1 minute:
```
Terminal 1 - Docker:
✅ Platform started successfully!
📊 Available Services:
  • Kafka Broker    → localhost:9092
  • Streamlit       → http://localhost:8501

Terminal 2 - Producer:
Connected to Kafka broker...
Publishing messages...
Message 1/100: Video ABC123...
Message 2/100: Video XYZ789...

Browser:
Streamlit dashboard loads with empty charts
```

### After 2 minutes:
```
Terminal 2:
Message 10/100: Video ABC456...
Message 11/100: Video XYZ012...

Browser:
Charts start showing data points
"Avg Views: 50,000"
"Data Points: 10"
Line chart shows upward trend
```

### After 5 minutes:
```
Terminal 2:
Message 30/100: Video ABC789...

Browser:
All charts populated with data
"Avg Views: 52,500"
"Data Points: 30"
Distribution histograms showing bell curves
Latest videos tab shows 10 videos
```

---

## 🔧 Configuration

### Change Kafka Topic
Edit producer command:
```bash
# Default topic is "youtube_videos"
python app/producer_youtube.py --topic my_topic --loop
```

### Change Update Interval
```bash
# Publish every 30 seconds (default is 10)
python app/producer_youtube.py --interval 30 --loop
```

### Change Number of Messages
```bash
# Send 50 messages instead of infinite loop
python app/producer_youtube.py --count 50
```

### Adjust Chart Refresh Rate
Edit `app/streamlit_dashboard.py` line 155:
```python
# Change consumer_timeout_ms for different refresh rates
consumer_timeout_ms=1000,  # 1 second (default)
# Change to 2000 for 2 seconds, etc.
```

---

## 📚 Documentation

- **Full Docker Guide**: [DOCKER_SETUP.md](DOCKER_SETUP.md)
- **Streamlit Features**: [STREAMLIT_README.md](STREAMLIT_README.md)
- **Implementation Details**: [IMPLEMENTATION_SUMMARY.md](IMPLEMENTATION_SUMMARY.md)
- **Quick Reference**: [QUICK_START.sh](QUICK_START.sh)

---

## 🆘 Emergency Troubleshooting

### Everything is broken, start fresh:
```bash
# Stop everything
docker-compose down -v

# Start over
./start-docker.sh
```

### View all Docker errors:
```bash
# Check all container logs
docker-compose logs --tail=100

# Or specific service
docker-compose logs kafka --tail=50
docker-compose logs streamlit --tail=50
```

### Manual Kafka connectivity test:
```bash
# Connect to Kafka container and test
docker-compose exec kafka bash

# Inside container:
kafka-topics --list --bootstrap-server localhost:9092
kafka-console-consumer --bootstrap-server localhost:9092 --topic youtube_videos --from-beginning --max-messages 5
```

### Check Resource Usage:
```bash
# View memory/CPU usage
docker stats

# If memory is low, reduce Spark workers:
# Edit docker-compose.yml, change SPARK_WORKER_MEMORY from 2G to 1G
# Then restart: docker-compose restart spark-worker-1
```

---

## ✅ Success Indicators

✓ You know it's working when:
1. ✅ Producer shows "Message X/100: Video..."
2. ✅ Streamlit dashboard loads at http://localhost:8501
3. ✅ Line charts show data points updating
4. ✅ "Data Points" counter increases
5. ✅ "Latest Videos" tab shows incoming videos
6. ✅ Kafka UI shows messages in `youtube_videos` topic

---

## 🎯 Next Steps

After basic setup works:

1. **Explore Kafka UI**: http://localhost:8080
2. **View Spark Jobs**: http://localhost:8081
3. **Modify Dashboard**: Edit `app/streamlit_dashboard.py`
4. **Add Custom Metrics**: Add calculations to `create_*` functions
5. **Deploy to Production**: Use `docker-compose.yml` on cloud server

---

## 📞 Quick Help

| Issue | Command |
|-------|---------|
| Services won't start | `./start-docker.sh` |
| Check status | `docker-compose ps` |
| View logs | `docker-compose logs -f` |
| Restart Kafka | `docker-compose restart kafka` |
| Stop everything | `docker-compose down` |
| Full reset | `docker-compose down -v` |
| Help | `./QUICK_START.sh` |

---

## 🎓 Learning Path

1. **Day 1**: Get dashboard running ← **You are here**
2. **Day 2**: Explore Kafka UI and topics
3. **Day 3**: Customize Streamlit dashboard
4. **Day 4**: Add custom metrics and calculations
5. **Day 5**: Integrate Spark streaming jobs

---

**Ready?** Start with: `./start-docker.sh` 🚀
