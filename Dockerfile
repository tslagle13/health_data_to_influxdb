FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the script
COPY Fitbit_Fetch.py .

# Create directories for logs and tokens
RUN mkdir -p /app/logs /app/tokens

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Run the script
CMD ["python", "Fitbit_Fetch.py"]

# Healthcheck to restart container if script is not running
HEALTHCHECK --interval=30s --timeout=10s --retries=3 CMD sh -c "ps aux | grep Fitbit_Fetch.py | grep -v grep || exit 1"
