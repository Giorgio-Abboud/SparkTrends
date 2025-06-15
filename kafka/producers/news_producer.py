import os
import logging
import requests
from dotenv import load_dotenv

load_dotenv()  # Later change to dynamically pick the correct environment

# -----

# Setup logging to print errors if message sending fails
logging.basicConfig(level=logging.INFO)  # Logs messages with a level of INFO or higher (ignores DEBUG)
log = logging.getLogger(__name__)  # Retrieves logger instance specific to the current module

# Load the AlphaVantage API key and the Kafka Broker from the env
ALPHA_API_KEY = os.environ["ALPHA_API_KEY"]
# BOOTSTRAP_SERVER = os.environ["KAFKA_BROKER"]

# -----

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

# -----

# Pushing the news article dictionaries to the broker
def process_news_ticker(ticker, company, sector, topic, producer):

    log.info(f"Fetching news for {ticker} ({company}) in the {sector} sector")
    articles = stream_news_for_ticker(ticker)

    for article in articles:
        # Attaching extra metadata to more easilty filter and search through
        article["ticker"] = ticker
        article["company"] = company
        article["sector"] = sector

        log.info(f"Sending {ticker} to {topic} topic for article: {article['title'][:40]}")  # See 40 characters of the article being sent

        producer.send(topic, value=article)\
            .add_callback(successful_send)\
            .add_errback(send_error)
        
# -----

# Called when a message is successfully sent to Kafka
def successful_send(record_metadata):
    print(f"Sent to {record_metadata.topic} | Partition: {record_metadata.partition} | Offset: {record_metadata.offset}")

# Called when there is an error sending a message to Kafka
def send_error(excp):
    log.error('Failed to send message to Kafka: ', exc_info=excp)
