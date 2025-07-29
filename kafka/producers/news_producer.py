import os, logging
from datetime import datetime, timezone
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

# Fetch the news articles that contain a specific symbol
async def batch_news(symbol, session):
    # Build the url to make the correct API call
    url = f"https://www.alphavantage.co/query?function=NEWS_SENTIMENT&symbols={symbol}&apikey={ALPHA_API_KEY}"

    # Will fetch today's crypto data using the API's "Last Refreshed" timestamp
    # Fetches the news-sentiment feed and returns the list of articles or [] on error
    try:
        async with session.get(url) as response:
            if response.status != 200:
                log.error(f"News API bad status for {symbol}: {response.status}")
                return []
            
            raw = await response.json()  # Retreive all the data for now

    except Exception as e:
        log.error(f"Failed fetching article data for {symbol}: {e}")
        return []  # Return an empty list in case an error was caught

    # Get today's payload
    articles = raw.get("feed", [])
    date_key = datetime.now(timezone.utc).strftime("%Y%m%d")

    today_articles = [
        art for art in articles
        if art.get("time_published", "").startswith(date_key)
    ]

    if not today_articles:
        log.warning(f"No article data for {symbol} on {date_key}")
    return today_articles

# -----

# Pushing the news article dictionaries to the broker
async def publish_news(symbol, name, sector, topic, producer):

    log.info(f"Fetching news for {symbol} ({name}) in the {sector} sector")
    articles = await batch_news(symbol)

    for article in articles:
        # Attaching extra metadata to more easilty filter and search through
        message = {
            "symbol" : symbol,
            "name": name,
            "sector": sector,
            "time_published": article['time_published'],
            "news_info": article
        }

        log.info(f"Sending article for {symbol}: {article['title'][:40]}…")  # See 40 characters of the article being sent

        # Non-blocking send with explicit callback handling
        try:
            record_metadata = await producer.send_and_wait(topic, message)
            successful_send(record_metadata)
        except Exception as excp:
            send_error(excp)
        
# -----

# Called when a message is successfully sent to Kafka
def successful_send(record_metadata):
    print(f"Sent to {record_metadata.topic} | Partition: {record_metadata.partition} | Offset: {record_metadata.offset}")

# Called when there is an error sending a message to Kafka
def send_error(excp):
    log.error('Failed to send message to Kafka: ', exc_info=excp)
