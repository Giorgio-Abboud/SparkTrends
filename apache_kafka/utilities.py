import os, json, logging, asyncio, time
from aiokafka import AIOKafkaProducer
from kafka.admin import KafkaAdminClient
from kafka.errors import TopicAlreadyExistsError
from dotenv import load_dotenv

load_dotenv()  # Later change to dynamically pick the correct environment

# -----

# Setup logging to print errors if message sending fails
logging.basicConfig(level=logging.INFO)  # Logs messages with a level of INFO or higher (ignores DEBUG)
log = logging.getLogger(__name__)  # Retrieves logger instance specific to the current module

# Load the Kafka Broker from the env
BOOTSTRAP_SERVER = os.environ["KAFKA_BROKER"]

# -----

# Function to attempt connecting to Kafka with retry logic (in case the producer runs before the broker is up)
async def connect_to_kafka(max_retries=6, delay=10):
    # Tries to connect to kafka 'max_retries' amount of times
    for attempt in range(max_retries):
        try:
            log.info(f"Attempt {attempt + 1}: Connecting to Kafka")

            # Attempt to create a Kafka producer instance and raises an exception if Kafka isn't reachable
            producer = AIOKafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVER,                       # Address of the Kafka broker
                value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serialize Python dicts to JSON bytes before sending to Kafka
            )

            await producer.start()

            log.info("Kafka connection established")

            return producer
        
        except Exception as e:
            # Log the error and wait before retrying
            log.error(f"Connection failed: {e}")
            await asyncio.sleep(delay)

    # If the loop ends and no producer was created raise and error
    raise RuntimeError("Kafka broker status not healthy after multiple attempts.")

# -----

def create_topics(topics, timeout=45.0, interval=5.0):
    # Create an admin client to handle topic creation
    admin_client = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVER)

    # Attempt the creation of topics, handle errors, and close the admin client when done
    try:
        admin_client.create_topics(new_topics=topics, validate_only=False)
        log.info(f"Requested creation for topics: {[t.name for t in topics]}")

        # Setup when it is taking too long to put up the topics
        deadline = time.monotonic() + timeout

        # Continue until we hit the exception
        while True:
            # Get all active topics
            active_topics = admin_client.list_topics()
            # Check for any missing topics
            missing = [t.name for t in topics if t.name not in active_topics]

            # If all are active break out of the loop
            if not missing:
                log.info(f"All topics are operational: {[t.name for t in topics]}")
                break
            # If any are missing when the deadline is passed
            if time.monotonic() > deadline:
                raise TimeoutError(f"Topics {missing} not visible after {timeout}s")
            
            # Otherwise put the function to sleep for interval number of seconds
            log.info(f"Waiting for topics to appear: {missing}, checking again in {interval}s")
            time.sleep(interval)

    except TopicAlreadyExistsError:
        log.warning("Topic already exists")
        
    finally:
        admin_client.close()
