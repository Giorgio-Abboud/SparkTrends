from kafka import KafkaProducer
import os
import json
import logging
from newsapi import NewsApiClient
from datetime import datetime, timedelta
from time import sleep  # Used to simulate delay between messages (subject to change)
from dotenv import load_dotenv

load_dotenv()

# Setup logging to print errors if message sending fails
logging.basicConfig(level=logging.INFO)  # Logs messages with a level of INFO or higher (ignores DEBUG)
log = logging.getLogger(__name__)  # Retrieves logger instance specific to the current module

newsapi = NewsApiClient(api_key=os.environ["NEWS_API_KEY"]) # Retrieving the api key from .env

BOOTSTRAP_SERVER = os.environ["KAFKA_BROKER"]

# Function to attempt connecting to Kafka with retry logic (kafka_api_producer is running before kafka)
def connect_to_kafka(max_retries=10, delay=3):
    # Tries to connect to kafka 'max_retries' amount of times
    for attempt in range(max_retries):
        try:
            log.info(f"Attempt {attempt + 1}: Connecting to Kafka")

            # Attempt to create a Kafka producer instance
            # This will raise an exception if Kafka isn't reachable
            producer = KafkaProducer(
                # Address of the Kafka broker
                bootstrap_servers=BOOTSTRAP_SERVER,

                # Serialize Python dicts to JSON bytes before sending to Kafka
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )

            log.info("Kafka connection established")

            return producer
        
        except Exception as e:
            # Log the error and wait before retrying
            log.error(f"Connection failed: {e}")
            sleep(delay)

    # If every attempt fails raise an error to stop the script
    raise RuntimeError("Kafka broker not available after multiple attempts.")

def stream_news_from_api(topic, companies, producer):
    # Get 7 days ago (or adjust as needed)
    earliest_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')

    for company in companies:
        try:
            response = newsapi.get_everything(
                q=company,
                from_param=earliest_date,
                language='en',
                sort_by='publishedAt',
                page_size=10
            )

            articles = response.get("articles", [])

            for article in articles:
                article['company'] = company
                print(f"Sending [{company}] to {topic}: {article['title']}") # To see where the code is going
                producer.send(topic, value=article)\
                        .add_callback(successful_send)\
                        .add_errback(send_error)
                sleep(1)

        except Exception as e:
            log.error(f"Error fetching news for {company}:", exc_info=e)

# Called when a message is successfully sent to Kafka
def successful_send(record_metadata):
    print(f"Sent to {record_metadata.topic} | Partition: {record_metadata.partition} | Offset: {record_metadata.offset}")

# Called when there is an error sending a message to Kafka
def send_error(excp):
    log.error('Failed to send message to Kafka: ', exc_info=excp)

# Main execution
if __name__ == "__main__":
    # Also used for test json data
    # # Send news articles to current_news topic
    # send_data('test_data/news.json', 'current_news')

    # # Send stock records to current_stock topic
    # send_data('test_data/stocks.json', 'current_stock')

    producer = connect_to_kafka() # Ensures that the producer is not created until kafka is running
    tracked_companies = ["Apple", "Tesla", "Microsoft"]
    stream_news_from_api('current_news', tracked_companies, producer)

    # Wait until all buffered messages are sent
    producer.flush()

    # Close the producer cleanly
    producer.close()
