FROM python:3.11-slim-bookworm

# Sets the working directory for the following instructions
WORKDIR /app

# Copy your root-level requirements
COPY requirements.txt .

# Install Python dependencies
RUN ["pip3", "install", "--no-cache-dir", "-r", "requirements.txt"]

# Copy your Kafka scripts and json files into the image
# COPY <src> <dest> where src if from my dir and dest is where it will be in the image
# Copy all your code into the image
COPY kafka/           ./kafka/
COPY kafka/producers/ ./producers/
COPY kafka/consumers/ ./consumers/
COPY tracked_data/    ./tracked_data/
COPY spark/           ./spark/