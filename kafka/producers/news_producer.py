from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
import os
import json
import logging
import requests
from time import sleep
from dotenv import load_dotenv

load_dotenv()

# Setup logging to print errors if message sending fails
logging.basicConfig(level=logging.INFO)  # Logs messages with a level of INFO or higher (ignores DEBUG)
log = logging.getLogger(__name__)  # Retrieves logger instance specific to the current module

# Load the AlphaVantage API key and the Kafka Broker from the env
ALPHA_API_KEY = api_key=os.environ["ALPHA_API_KEY"]
BOOTSTRAP_SERVER = os.environ["KAFKA_BROKER"]

# ---

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

# ---

# Load the companies that will be tracked
def load_tracked_companies(file="tracked_data/tracked_companies"):
    with open(file, "r") as f:
        tracker = json.load(f)
    
    # Flatten json dictionaries to ensure easier data processing and analysis
    flatten = [
        (sector, data["company"], data["ticker"])
        for sector, map in tracker.items()
        for data in map
    ]

    return flatten

# ---

# Fetch the news articles that contain a specific ticker
def stream_news_for_ticker(ticker):
    # Build the url to make the correct API call
    url = f"https://www.alphavantage.co/query?function=NEWS_SENTIMENT&tickers={ticker}&apikey={ALPHA_API_KEY}"

    # Error handling in case of a bad GET request
    try:
        response = requests.get(url)
        if response.status_code == 200:  # Ensuring that the request was successful
            return response.json().get("feed", [])
        else:
            log.error(f"Unsuccessful response for {ticker}: {response.status_code}")
    except Exception as e:
        log.error(f"Failed fetching news for {ticker}: {e}")

    return []  # Return an empty list in case an error was caught

# ---

# Pushing the news article dictionaries to the broker
def stream_news_from_api(topic, producer):
    companies = load_tracked_companies()  # Retrieve the flattened data

    for sector, company, ticker in companies:
        log.info("Fetching news for {ticker} ({company}) in the {sector} sector")
        articles = stream_news_for_ticker(ticker)

        for article in articles:
            # Attaching extra metadata to more easilty filter and search through
            article["ticker"] = ticker
            article["company"] = company
            article["sector"] = sector

            log.info(f"Sending {ticker} to {topic} topic for article: {article["title"][:40]}")  # See 40 characters of the article being sent

            producer.send(topic, value=article)\
                .add_callback(successful_send)\
                .add_callback(send_error)
            
            sleep(1)  # To slow the flow of data (easier to debug for now)

        sleep(12)  # To account for AlphaVantage's 5 requests per minute for the free tier

# ---

# Called when a message is successfully sent to Kafka
def successful_send(record_metadata):
    print(f"Sent to {record_metadata.topic} | Partition: {record_metadata.partition} | Offset: {record_metadata.offset}")

# Called when there is an error sending a message to Kafka
def send_error(excp):
    log.error('Failed to send message to Kafka: ', exc_info=excp)

# ---

# Main execution
if __name__ == "__main__":
    producer = connect_to_kafka()
    stream_news_from_api('current_news', producer)
    producer.flush()  # Wait until all buffered messages are sent
    producer.close()  # Close the producer cleanly
