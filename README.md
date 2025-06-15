# SparkTrends

**SparkTrends** is a **Market Trends Intelligence Platform** that ingests, processes, and analyzes financial market data, including company headlines, stock prices, and cryptocurrency trends, to detect emerging market patterns. It simulates a scalable and real-time pipeline to provide insight, powered by Kafka, Spark Airflow, Docker, and PostgreSQL.


## Project Goal

In today's world, **data is knowledge**, and how quickly you are able to obtain that insight could determine whether you capitalize or let an opportunity slip away. The goal at SparkTrends is to decipher data cultivated from online sources and processing it into insight that offers a tactical market advantage.

SparkTrends aims to:
- Continuously gather data from API sources.
- Detect sudden market trends and price changes.
- Implement **sentiment analysis** alongside financial metrics.
- Power an intuitive dashboard to facilitate data visualization.


## Input & Output

### Input Sources
- **Source**
  - Alpha Vantage API.
- **Description**
  - Daily data is fetched for company news, stock prices, and cryptocurrency prices through scheduled API calls.
- **Format**
  - Raw input received as Python **list** or **dictionary** objects.
  - The original data is serialized as **JSONB** and stored in PostgreSQL.
  - Post processing with Apache Spark transforms and flattens the data before storing it in structured PostgreSQL tables with appropriate typed columns.

### Output Destinations
- **Destination**
  - PostgreSQL
  - Dashboard (Future plans)
- **Description**
  - Cleaned company news, stock prices, and cryptocurrency prices data.
  - Daily updates of trends and metrics.
- **Format**
  - Tabular SQL
  - Visual dashboard


## Tech Stack

- **Apache Kafka** – Daily ingestion of market news, and stock and cryptocurrency prices. 
- **Apache Spark** – Scalable data processing and NLP trend tagging.
- **Apache Airflow** – Workflow orchestration for batch pipelines.
- **PostgreSQL** – Structured storage for processed trend data.
- **Docker** - Full-stack containerized environment with dev/prod configs.
- **Flask** – Hosts a web server with RESTful APIs.
- **Python** – ETL logic, NLP, and Flask backend.


## Architecture Diagram

[API Data] -> [Kafka Producers] -> [Kafka Consumers] -> [PostgreSQL] -> [Apache Spark] -> [PostgreSQL] -> [Future Dashboard]
The diagram will be orchestrated by Apache Airflow, which will run these tasks daily.


## Project Phases

### **Phase 1: Infrastructure & Kafka (In Progress)**
- `init_project.py` generates `.env`, `.env.example`, `docker-compose.yml`, and `requirements.txt`.
- Set up PostgreSQL and Kafka using container images, including **Chainguard secure container images**.
- Volume persistence and environment-aware Docker configs.
- Compile json files containing essential data needed for API calls.
- Centralized topic creator file to eliminate redundant code.
- Centralized producer file to execute producers and consumers.
- Kafka Producer ingests formatted news, stock and crypto data from an API.
- Kafka consumer stores parsed records in PostgreSQL [Coming Soon]


## Future Works In Progress ⚙️🚧
