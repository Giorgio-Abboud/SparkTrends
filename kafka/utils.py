from producers import news_producer, stock_producer, crypto_producer
from consumers import consumer
from kafka import KafkaProducer
import os
import json
import logging
from time import sleep
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
def connect_to_kafka(max_retries=5, delay=5):
    # Tries to connect to kafka 'max_retries' amount of times
    for attempt in range(max_retries):
        try:
            log.info(f"Attempt {attempt + 1}: Connecting to Kafka")

            # Attempt to create a Kafka producer instance and raises an exception if Kafka isn't reachable
            producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVER,  # Address of the Kafka broker
                value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serialize Python dicts to JSON bytes before sending to Kafka
            )

            log.info("Kafka connection established")
            return producer
        
        except Exception as e:
            # Log the error and wait before retrying
            log.error(f"Connection failed: {e}")
            sleep(delay)

    # If the loop ends and no producer was created raise and error
    raise RuntimeError("Kafka broker status not healthy after multiple attempts.")

# -----

# Load the companies that will be tracked
def load_tracked_companies(file="tracked_data/tracked_companies.json"):
    with open(file, "r") as f:
        tracker = json.load(f)
    
    # Flatten json dictionaries to ensure easier data processing and analysis
    flatten = [
        (sector, data["company"], data["ticker"])
        for sector, map in tracker.items()
        for data in map
    ]

    return flatten

# -----

def load_tracked_crypto(file="tracked_data/tracked_crypto.json"):
    with open(file, "r") as f:
        tracker = json.load(f)
    
    # Flatten json dictionaries to ensure easier data processing and analysis
    flatten = [
        (category, data["crypto"], data["symbol"])
        for category, map in tracker.items()
        for data in map
    ]

    return flatten

# -----

# Function for batch processing
def batch_process():
    # Start by creating a producer instance
    producer = connect_to_kafka()

    tracked_data = []  # List to store the data for the resquests to be made
    flat_company_news = load_tracked_companies()  # Load the flattened data for companies and stocks
    flat_crypto = load_tracked_crypto()  # Load the flattened data for crypto

    for sector, company, ticker in flat_company_news:  # Adding a tuple with the necessary data for news
        tracked_data.append(("news", sector, company, ticker, "current_news"))

    for sector, company, ticker in flat_company_news:  # Same for stock
        tracked_data.append(("stocks", sector, company, ticker, "current_stock"))

    for category, crypto, symbol in flat_crypto:  # Same for crypto
        tracked_data.append(("crypto", category, crypto, symbol, "current_crypto"))

    # Call the respective producers and provide the necessary data
    for type, sector, company, ticker, topic in tracked_data:

        if type == "news":
            news_producer.process_news_ticker(ticker, company, sector, topic, producer)

        if type == "stocks":
            stock_producer.process_stock_ticker(ticker, company, sector, topic, producer)

        elif type == "crypto":
            crypto_producer.process_crypto_symbol(ticker, company, sector, topic, producer)

        sleep(12)  # Make sure to get 5 requests per minute (5 request limit for AlphaVantage)

    producer.flush()  # Wait until all buffered messages are sent
    producer.close()  # Close the producer cleanly

# -----

if __name__ == "__main__":
    batch_process()
