FROM python:3.11-slim

WORKDIR /app

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the scripts
COPY bq_inventory_sync.py .

# Set default environment variables (can be overridden in Cloud Run)
ENV PARENT_ID=""
ENV LOOKBACK=1
ENV CONCURRENCY=10

# Run the script using environment variables if no args provided
ENTRYPOINT ["python", "bq_inventory_sync.py"]
