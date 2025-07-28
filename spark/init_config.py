import os
import yaml
from dotenv import load_dotenv

load_dotenv()

# PostreSQL configs
HOST=os.environ["POSTGRES_HOST"]
DB_NAME=os.environ["POSTGRES_DB"]
USER=os.environ["POSTGRES_USER"]
PASSWORD=os.environ["POSTGRES_PASSWORD"]
PORT=int(os.environ["POSTGRES_PORT"])

# Kafka configs
BOOTSTRAP_SERVER = os.environ["KAFKA_BROKER"]
         
config_template = {
    'jdbc': {
        'url': f'jdbc:postgresql://{HOST}:{PORT}/{DB_NAME}?user={USER}&password={PASSWORD}',
        'driver': 'org.postgresql.Driver'  # Java Database Connectivity driver enables PySpark apps to connect with relational databases
    },
    'spark': {
        'master': 'local[*]',  # Will use all cores on the local machine
        'app_name': 'SparkTrendsApp'
    },
    'window': {
        'weekly_volatility': 7,
        'horizon_days': 1
    },
    'model': {
        'model_path': 'models',
        'test_dec': 0.2,
        'seed': 6,
        'csv_path': 'data/data_stocks.csv'
    },
    'kafka': {
        'kafka_bootstrap_servers': BOOTSTRAP_SERVER
    }
}

# False enforces a block style which uses indentation to represent structure
with open("config.yml", "w") as f:
    yaml.dump(config_template, f, default_flow_style=False)

print("The config.yml file was created")