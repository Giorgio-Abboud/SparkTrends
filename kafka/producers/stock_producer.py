import os
import logging
import requests
from time import sleep
from dotenv import load_dotenv

load_dotenv()  # Later change to dynamically pick the correct environment

# -----

# Setup logging to print errors if message sending fails
logging.basicConfig(level=logging.INFO)  # Logs messages with a level of INFO or higher (ignores DEBUG)
log = logging.getLogger(__name__)  # Retrieves logger instance specific to the current module

# Load the AlphaVantage API key and the Kafka Broker from the env
ALPHA_API_KEY = os.environ["ALPHA_API_KEY"]

# -----

# Fetch the stock market values that contain a specific ticker
def stream_stocks_for_ticker(ticker):
    # Build the url to make the correct API call
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol={ticker}&apikey={ALPHA_API_KEY}"

    # Error handling in case of a bad GET request
    try:
        response = requests.get(url)
        if response.status_code == 200:  # Ensuring that the request was successful
            return response.json().get("Time Series (Daily)", {})
        else:
            log.error(f"Unsuccessful response for {ticker}: {response.status_code}")
    except Exception as e:
        log.error(f"Failed fetching stock data for {ticker}: {e}")

    return {}  # Return an empty list in case an error was caught

# -----

# Pushing the stock quotes dictionaries to the broker
def process_stock_ticker(ticker, company, sector, topic, producer):

    log.info(f"Fetching stocks for {ticker} ({company}) in the {sector} sector")
    quotes = stream_stocks_for_ticker(ticker)

    for date, quote in quotes.items():
        # Attaching extra metadata to more easilty filter and search through
        quote["ticker"] = ticker
        quote["company"] = company
        quote["sector"] = sector
        quote["date"] = date  # Already present in the key but easier to process as a value

        log.info(f"Sending {ticker} to {topic} topic for quote on {date}")

        producer.send(topic, value=quote)\
            .add_callback(successful_send)\
            .add_errback(send_error)
        
# -----

# Called when a message is successfully sent to Kafka
def successful_send(record_metadata):
    print(f"Sent to {record_metadata.topic} | Partition: {record_metadata.partition} | Offset: {record_metadata.offset}")

# Called when there is an error sending a message to Kafka
def send_error(excp):
    log.error('Failed to send message to Kafka: ', exc_info=excp)
