import os
import yaml
import subprocess
import getpass

# ----------------------------------------
# 1. Creating the .env.example file
# ----------------------------------------

# Create a .env file for the developer, .env.example for an example to put on GitHub, and another for production
# Only .env.example will not be gitignored as it is safe for GitHub
env_example_path = ".env.example"
default_env_content = """POSTGRES_USER=your_postgres_user
POSTGRES_PASSWORD=your_secure_password
POSTGRES_DB=financial_database
"""

# Write the content into the .env file
with open(env_example_path, "w") as f:
    f.write(default_env_content)

print("The .env.example file was created and is safe to commit to GitHub")


# ----------------------------------------
# 2. Creating the .env file
# ----------------------------------------

# The .env file will be created, if it does not yet exist, using manually inputted information from the user
env_path = ".env"

if not os.path.exists(env_path): # If not found in the directory
    print("Creating a new .env file that will not be pushed to GitHub.\n")

    user = input("POSTGRES_USER: ").strip() # Strip to remove any leading or trailing white spaces
    pw = getpass.getpass("POSTGRES_PASSWORD: ").strip()
    db = input("POSTGRES_DB: ").strip()

    # Safely store this data in the .env file
    with open(env_path, "w") as f:
        f.write(f"POSTGRES_USER={user}\n")
        f.write(f"POSTGRES_PASSWORD={pw}\n")
        f.write(f"POSTGRES_DB={db}\n")

    print("The .env file created for developper use")
else:
    print("A .env file already exists")


# ----------------------------------------
# 3. Generate docker-compose.yml for dev/prod
# ----------------------------------------

# This lets us dynamically change ports, hostnames, and volume names for either developer or production mode
dev_mode = input("Is this for development? (y/n): ").lower() == "y"

# Dynamically assign different volume names to allow data isolation between dev DB and prod DB
pg_volume_name = 'pg_data_dev' if dev_mode else 'pg_data_prod'
kafka_volume_name = 'kafka_data_dev' if dev_mode else 'kafka_data_prod'


# ----------------------------------------
# I. Create the postgres service
# ----------------------------------------

# Define the PostgreSQL service spec
postgres_service = {
    'image': 'cgr.dev/chainguard/postgres:latest',  # Secure and minimal Postgres image by Chainguard
    'container_name': 'market-postgres',  # Name to easily reference the container

    'restart': 'unless-stopped',  # This ensures Postgres auto-restarts if it crashes or after a reboot

    # Environment variables tell Postgres what user, password and DB to create
    # Values like ${POSTGRES_USER} are read from a local .env file (will not be publicly available)
    'environment': {
        'POSTGRES_USER': '${POSTGRES_USER}',
        'POSTGRES_PASSWORD': '${POSTGRES_PASSWORD}',
        'POSTGRES_DB': '${POSTGRES_DB}',
    },

    # Restart policy ensures the container auto-restarts if it crashes or Docker restarts
    'restart': 'always',

    # Mount volumes:
    # - A named volume to persist database data across container restarts
    # - A bind mount to run init.sql on first-time setup (only if volume is empty)
    'volumes': [
        f'{pg_volume_name}:/var/lib/postgresql/data',  # Persistent database storage
        './postgres/init.sql:/docker-entrypoint-initdb.d/init.sql'  # One-time schema init
    ],

    # Assign the Postgres service to the custom network for inter-container communication
    'networks': ['sparktrends_net']
}

# In dev mode the internal container port 5432 is exposed to the host so local apps connect to Postgres
if dev_mode:
    postgres_service['ports'] = ['5432:5432']


# ----------------------------------------
# II. Create the kafka service
# ----------------------------------------

# Define the Kafka service (KRaft so no need for ZooKeeper)
# This Kafka image uses KRaft mode and acts as both broker (node that stores and manages data streams) and controller (manages the metadata)
kafka_service = {
    'image': 'apache/kafka:latest',  # Chainguard's secure and minimal Kafka with KRaft image
    'container_name': 'kafka',

    'restart': 'unless-stopped',  # This ensures Kafka auto-restarts if it crashes or after a reboot

    # Environment variables configuring Kafka KRaft mode
    'environment': {
        'KAFKA_NODE_ID': 1,  # Each node must have a unique numeric ID
        'KAFKA_PROCESS_ROLES': 'broker,controller',  # This node handles both broker and controller roles (enough for smaller scale projects)

        # Defines the listeners inside the container:
        # Listeners are crucial for both consumers and producers to connect and interact with the Kafka broker
        # - PLAINTEXT: for producers/consumers (subject to change)
        # - CONTROLLER: for internal controller coordination
        'KAFKA_LISTENERS': 'PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093',

        # This is what external apps, like a Python client, use to connect to Kafka
        # Using the default service name "kafka" which works inside the Docker network
        'KAFKA_ADVERTISED_LISTENERS': 'PLAINTEXT://kafka:9092',

        # Tells Kafka which listener is used for controller communication
        'KAFKA_CONTROLLER_LISTENER_NAMES': 'CONTROLLER',

        # Maps listener names to protocols and all are PLAINTEXT in this simple setup (subject to change)
        'KAFKA_LISTENER_SECURITY_PROTOCOL_MAP': 'PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT',

        'KAFKA_INTER_BROKER_LISTENER_NAME': 'PLAINTEXT',

        # Tells this node who participates in the controller quorum
        # Format: <node_id>@<hostname>:<port> and once again uses default host name "kafka"
        'KAFKA_CONTROLLER_QUORUM_VOTERS': '1@kafka:9093',

        # Required for Kafka to successfully create internal consumer offsets topic with only one broker
        'KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR': 1,

        # Reduces consumer group startup lag (removes the default 3 second delay when a new consumers join)
        'KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS': 0,

        # Minimum number of ISR (in-sync replicas) required to accept writes to transaction logs (1 in a single-node setup)
        'KAFKA_TRANSACTION_STATE_LOG_MIN_ISR': 1,

        # Required for enabling transactions (Kafka producers and consumers) with a single broker
        'KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR': 1,

        # Ensure that every topic is manually created
        'KAFKA_AUTO_CREATE_TOPICS_ENABLE': "false",

        # Kafka’s internal storage location for logs, metadata, and more
        'KAFKA_LOG_DIRS': '/var/lib/kafka/data'
    },

    # Persist Kafka data in a named volume (separate for dev and prod)
    'volumes': [f'{kafka_volume_name}:/var/lib/kafka/data'],

    # Assign the Kafka service to the same network as Postgres so other containers can resolve its hostname
    'networks': ['sparktrends_net'],

    'healthcheck': {
        'test': ['CMD', 'sh', '-c', 'nc -z localhost 9092'],
        'interval': '10s',
        'timeout': '5s',
        'retries': 10
    }
}

# In dev mode override the listener addresses to use localhost since it is easier to run Kafka tools or clients on a host machine
if dev_mode:
    kafka_service['environment']['KAFKA_ADVERTISED_LISTENERS'] = 'PLAINTEXT://kafka:9092'
    kafka_service['environment']['KAFKA_CONTROLLER_QUORUM_VOTERS'] = '1@kafka:9093'

    # Expose Kafka’s port 9092 to the host like for local Python scripts
    kafka_service['ports'] = ['9092:9092']

env_file_path = ".env.production" if not dev_mode else ".env"


# ----------------------------------------
# III. Create the topic service
# ----------------------------------------

# Container ensuring topic creation
topic_creator_service = {
    'build': {
        'context': '.'
    },
    'container_name': 'topic_creator',
    'restart': 'on-failure',
    'command': ["python", "kafka/topics.py"],
    'depends_on': {
        'kafka': {
            'condition': 'service_healthy'
        }
    },
    'networks': ['sparktrends_net'],
    'env_file': env_file_path
}


# ----------------------------------------
# IV. Create the producer orchestration service
# ----------------------------------------

# Container to run the utility file which feeds data for the producers to make requests
producer_utility_service = {
    'build': {
        'context': '.'
    },
    'container_name': 'orchestrator',
    
    'restart': 'on-failure',  # Retry only on crashes

    'command': ["python", "kafka/utils.py"],

    # Ensure this waits until Kafka passes its healthcheck
    'depends_on': {
        'kafka': {
            'condition': 'service_healthy'
        },
        'topic_creator': {
            'condition': 'service_completed_successfully'
        }
    },
    'networks': ['sparktrends_net'],  # Attach to the same Docker network
    'env_file': env_file_path  # Load secrets like NEWS_API_KEY into the container
}


# ----------------------------------------
# V. Generate the yml file
# ----------------------------------------

# Assemble docker-compose content which includes both services and their named volumes
compose_config = {
    'services': {
        'postgres': postgres_service, # Include the Postgres service
        'kafka': kafka_service, # Include the Kafka service
        'topic_creator': topic_creator_service,
        'orchestrator': producer_utility_service
    },
    'volumes': {
        pg_volume_name: {}, # Register the Postgres volume
        kafka_volume_name: {} # Register the Kafka volume
    },
    'networks': {
        'sparktrends_net': { # An internal Docker network name
            # This makes sure services like Kafka, Postgres, and future API producers can talk to each other using service names
            'driver': 'bridge' # Default bridge driver ensures all services are on the same virtual network
            
        }
    }
}

# Convert the python dictionary into a YAML file and do not sort the dictionary keys
with open("docker-compose.yml", "w") as f:
    yaml.dump(compose_config, f, sort_keys=False)

print("The docker-compose.yml file was created")


# ----------------------------------------
# 4. Create the requirements.txt file
# ----------------------------------------

print("Fetching requirements.txt from the current environment")

# Will create a new file or overwrite the previous txt file and input the new requirements
with open("requirements.txt", "w") as f:
    # Runs the pip3 freeze command automatically
    subprocess.run(["pip3", "freeze"], stdout=f)
