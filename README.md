# SparkTrends

**SparkTrends** is a **real-time market trends intelligence platform** that ingests, processes, and analyzes financial market data like stocks, and soon crypto to uncover emerging patterns.

It simulates a production-style backend used in finance/fintech, powered by **Apache Kafka**, **Apache Spark**, **PostgreSQL**, and **Docker**.

---

## Project Goals

- **Continuously ingest** market data from APIs and WebSockets.  
- **Process & enrich** through Kafka and Spark.  
- **Compute core metrics** like returns, volatility, volume signals.  
- **Store** results in PostgreSQL for analysis.
- **Send alerts** via Slack when anomalies are detected

---

## Architecture

[API & WebSocket Producers] → [Kafka] → [Kafka Broker] → [Spark Jobs] → [PostgreSQL] → [Slack Alerts]

- **Tables:** Company Data, 1 Minute Interval Bars, and Computed Metrics

---

## Tech Stack

- **Apache Kafka** – Single-node KRaft broker for ingest and buffering.  
- **Apache Spark 4.0** – Batch transformation and streaming next.  
- **PostgreSQL** – Durable storage for processed data.  
- **Docker Compose** – Reproducible environment.  
- **Python** – Producers, Spark jobs, and orchestration scripts.

**Custom Spark image (dependencies to avoid misconfigurations)**

- `spark-sql-kafka-0-10_2.13:4.0.0`  
- `spark-token-provider-kafka-0-10_2.13:4.0.0`  
- `kafka-clients:3.6.1`  
- `commons-pool2:2.12.0` (required by Spark 4.0 Kafka connector)  
- **PostgreSQL JDBC:** `org.postgresql:postgresql:42.7.3`

The runner container is from the custom Spark image, so the driver and executors share the same classpath.

---

## What is Operational

- Docker Compose creates **Postgres**, **Kafka**, **Spark master/worker**, and a **Runner** that runs the job.  
- Spark **reads from the Kafka broker**, transforms OHLCV data, and **writes to Postgres**.
- Spark streaming job **sends alerts to Slack** upon anomaly detection.
- End-to-end: **API → Kafka → Spark → Postgres**.  
- Future Plans: implement different ways to detect deviations and the ingestion of crypto data.

---

## Quickstart

### 1) Prerequisites
- Docker Desktop / Docker Engine + Docker Compose  
- Optional Python 3.11+ if you want to run utilities locally

### 2) Environment
Create a `.env` file at the repo root (example):

```env
# Postgres
POSTGRES_USER=your_user
POSTGRES_PASSWORD=your_pass
POSTGRES_DB=your_db
POSTGRES_HOST=your_host
POSTGRES_PORT=your_port

# APIs
FINNHUB_API_KEY=your_key      # Stock Batch
TWELVE_DATA_API_KEY=your_key  # Stock Streaming
BIDANCE_API_KEY=your_key      # Crypto Streaming

# WebHook
SLACK_WEBHOOK=your_slack_webhook

# Kafka
KAFKA_BOOTSTRAP=your_kafka_port
```

### 3) Build and Run
- `docker compose down -v`
- `docker compose build --no-cache`
- `docker compose up -d --force-recreate`

### 4) Keep Track of your Logs
- Logs can be monitored thourgh the Docker Desktop
- `docker compose logs -f {YOUR_FILE}` if you input `docker compose up -d --force-recreate` in the terminal
- Inputting `docker compose up` (after building) since the `-d` hides logs

### 5) Verify in Postgres
Run the following command and check your tables
- docker exec -it market-postgres psql -U {YOUR_USER} -d {DB_NAME}
