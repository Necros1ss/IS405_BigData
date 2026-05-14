FROM python:3.11-slim

WORKDIR /app

# Install system dependencies including Java for Spark
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    openjdk-17-jre-headless \
    wget \
    netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt ./

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY app/ ./app/
COPY data/ ./data/ 2>/dev/null || true

# Expose ports for various services
EXPOSE 8080 8081 8082 7077 6066

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import sys; sys.exit(0)" || exit 1

# Default command
CMD ["python", "-u", "app/producer_youtube.py"]
