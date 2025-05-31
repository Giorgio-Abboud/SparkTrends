FROM python:3.11-slim-bookworm

WORKDIR /app

# Copy your root-level requirements
COPY requirements.txt .

# Install Python dependencies
RUN ["pip3", "install", "--no-cache-dir", "-r", "requirements.txt"]

# Copy your Kafka scripts into the image
COPY kafka/ ./kafka/

# Run the Kafka API producer script
# Fallback in case the commands in docker compose fail
CMD ["python", "kafka/producers/news_producer.py"]
