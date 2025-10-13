# Multi-stage build for Scheduling Report Worker - Python API
# Stage 1: Builder - Install dependencies
FROM python:3.11-slim as builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libmariadb-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir --user -r requirements.txt

# Stage 2: Runtime - Minimal image
FROM python:3.11-slim

WORKDIR /app

# Install runtime dependencies and timezone data
RUN apt-get update && apt-get install -y \
    libmariadb3 \
    tzdata \
    && rm -rf /var/lib/apt/lists/*

# Set timezone to Asia/Jakarta
ENV TZ=Asia/Jakarta
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Copy Python packages from builder
COPY --from=builder /root/.local /root/.local

# Copy application code
COPY . .

# Make sure scripts are in PATH
ENV PATH=/root/.local/bin:$PATH

# Set Python to run in unbuffered mode (recommended for containers)
ENV PYTHONUNBUFFERED=1

# Expose port (not used by worker, but good for health checks if added later)
EXPOSE 8000

# Run the Kafka consumer worker
CMD ["python3", "execution_engine/worker.py"]
