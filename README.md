# SparkCheck

**SparkCheck** is a real-time data engineering project designed to detect and flag deceitful misinformation using a data pipeline powered by Apache Kafka, Spark, Airflow, and NLP.

---

## Project Goal

To fight the rampant spread of false or misleading information online. With misinformation becoming increasingly abundant and potentially harmful, it's critical to identify and verify claims in real time.

---

## Tech Stack

- **Apache Kafka** – Real-time data ingestion  
- **Apache Spark** – Scalable data processing  
- **Apache Airflow** – Pipeline orchestration and monitoring  
- **PostgreSQL** – Structured data storage  
- **Streamlit** – Interactive frontend application  
- **Python** – Core logic and NLP (Natural Language Processing)

---

## Project Phases

### ✅ **Phase 1: Infrastructure (In Progress)**
- `init_project.py` to auto-generate `.env`, `.env.example`, `docker-compose.yml`, and `requirements.txt`
- PostgreSQL container using **Chainguard secure image**
- Environment-aware Docker configurations (dev vs prod)
- Persistent volume setup for database
- Kafka producer to ingest claims from JSON [Coming Soon]
- Kafka consumer to store in PostgreSQL [Coming Soon]

## Future Works In Progress ⚙️🚧
