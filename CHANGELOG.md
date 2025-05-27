# Changelog

All notable changes to this project as well as things I have learned will be documented in this file.

---

## Initial Infrastructure Setup (Phase 1)

- Created `init_project.py` to automate the creation of the project environment.
- Added secure `.env` management:
  - Automatically generates `.env.example` for GitHub
  - Prompts for user input to generate a real `.env`
- Integrated Chainguard’s secure PostgreSQL image.
- Dynamically generates `docker-compose.yml` with support for:
  - Dev mode: exposes port `5432`
  - Prod mode: omits port for security
- Added volume persistence using Docker named volumes (`pgdata`)
- Automatically generates `requirements.txt` using `pip3 freeze`
- Installed and configured:
  - `pyyaml`
  - `requests`
  - `subprocess`
  - `getpass`

- Learned:
  - `docker-compose down -v` deletes the persistent DB volume
  - `docker-compose down` should usually be run instead
  - Port exposure should be conditional on environment
  - Running `python3` vs `python` can affect which pip installation is used
  - The Chainguard kafka image requires authorized access to use
  - Kafka bootstrap server confusion
    - Inside Docker, kafka:9092 is the correct bootstrap server. Outside (on host), use localhost:9092 if port is exposed.
  - Using a custom docker network `sparktrends_net` so that the services (kafka, postgres, producer, consumer) communicate by hostname (kafka:9092)
    - Container DNS resolution: Inside the Docker network, the services can refer to each other by their container name
    - Isolated from other docker containers
    - Required for Kafka hostname since `KAFKA_ADVERTISED_LISTENERS` was set to `PLAINTEXT://kafka:9092`, that means the producer and consumer need to resolve kafka as a hostname, which only works if they share a Docker network.

- Useful Websites:
  - Kafka documentation: https://kafka.apache.org/documentation/#kraft_role
  - Running Kafka with docker: https://developer.confluent.io/confluent-tutorials/kafka-on-docker/#:~:text=connect%20to%20Kafka.-,Copy,configurations%2C%20consult%20the%20Kafka%20documentation.
  - Kafka Python Client: https://github.com/dpkp/kafka-python/tree/master?tab=readme-ov-file
  - Amazing example for consumer and producer: https://huzzefakhan.medium.com/apache-kafka-in-python-d7489b139384

  - NewsAPI: https://newsapi.org/
  - Finnhub: https://finnhub.io/docs/api/price-target
  - Alpha Vantage: https://www.alphavantage.co/documentation/
  - Stock Github: https://github.com/rreichel3/US-Stock-Symbols
  - Reddit link for future reference (APIs): https://www.reddit.com/r/algotrading/comments/1idhkr5/what_apis_are_you_guys_using_for_stock_data/

  - Kafka GitPod: https://github.com/devshawn/kafka-gitops

---

## Error Causes and Solution
- Kafka connection errors (ECONNREFUSED) on initial attempts
  - Cause: API producer was attempting to connect before the Kafka broker was ready.
  - Fix: Added healthcheck to Kafka container and added depends_on in docker-compose for the API producer to wait until Kafka is healthy.

- Kafka healthcheck failures
  - Cause: kafka-cluster.sh used in the healthcheck command is not available in the Apache Kafka image.
  - Fix: Replaced the command with a TCP socket test.

- No logs from API producer container
  - Cause: Producer was never triggered due to the health check condition not being satisfied.
  - Fix: Ensured it connects only after Kafka is healthy. Also fixed the previous health check issue.

- API producer not auto-starting
  - Fix: Once again ensured that the health check worked as intented. Logs were essential in figuring out this issue.

- Dockerfile CMD line not executing and directory not found
  - Cause: The `chainguard/python:latest-dev` image lacks a proper shell and entrypoint, causing the CMD `["python", "kafka/producers/producer.py"]` to not execute.
  - Fix: Switched the base image to `python:3.11-slim-bookworm`, which provides a standard shell environment and supports proper execution of Python scripts.