# SparkTrends

**SparkTrends** is a **Market Trends Intelligence Platform** which ingests, processes, and analyzes financial news and stock price data to detect changing market trends. It simulates a scalable and real-time backend pipeline similar to what would be used at companies.


## Project Goal

To uncover real-time market insights from ingested financial news and stock data, to analyze it for patterns and trends. It delivers summarized infromation to help its users monitor market shifts in one convenient dashboard.


## Tech Stack

- **Apache Kafka** – Real-time ingestion of financial news and stock price feeds 
- **Apache Spark** – Scalable data processing and NLP trend tagging
- **Apache Airflow** – Workflow orchestration for batch pipelines
- **PostgreSQL** – Structured storage for processed trend data
- **Docker** - Full-stack containerized environment with dev/prod configs
- **Flask** – Hosts a web server with RESTful APIs
- **Python** – ETL logic, NLP, and Flask backend


## Project Phases

### **Phase 1: Infrastructure (In Progress)**
- `init_project.py` generates `.env`, `.env.example`, `docker-compose.yml`, and `requirements.txt`
- Set up PostgreSQL and Kafka using **Chainguard secure container images**
- Volume persistence and environment-aware Docker configs
- Kafka Producer ingests formatted financial data [Coming soon]
- Kafka consumer stores parsed records in PostgreSQL [Coming Soon]


## Future Works In Progress ⚙️🚧
