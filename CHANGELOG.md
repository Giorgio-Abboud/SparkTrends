# Changelog

All notable changes to this project as well as things I have learned will be documented in this file.

---

## 2025-05-05
### Initial Infrastructure Setup (Phase 1)

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
- Issues Fixed:
  - `ModuleNotFoundError: No module named 'yaml'` by ensuring correct interpreter and environment (python instead of python3)

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