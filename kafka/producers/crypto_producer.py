import os, logging
from dotenv import load_dotenv

load_dotenv()  # Later change to dynamically pick the correct environment

# -----

# Setup logging to print errors if message sending fails
logging.basicConfig(level=logging.INFO)  # Logs messages with a level of INFO or higher (ignores DEBUG)
log = logging.getLogger(__name__)  # Retrieves logger instance specific to the current module

# Load the AlphaVantage API key and the Kafka Broker from the env
ALPHA_API_KEY = os.environ["ALPHA_API_KEY"]

# -----

# Fetch the crypto values that contain a specific symbol
async def batch_crypto_quote(symbol, session):
    # Build the url to make the correct API call (Market will be in USD) (Daily Cryptocurrencies)
    url = f"https://www.alphavantage.co/query?function=DIGITAL_CURRENCY_DAILY&symbol={symbol}&market=USD&apikey={ALPHA_API_KEY}"

    # Will fetch today's crypto data using the API's "Last Refreshed" timestamp
    try:
        async with session.get(url) as response:
            if response.status != 200:
                log.error(f"Crypto API bad status for {symbol}: {response.status}")
                return {}, ""
            
            raw = await response.json()  # Retreive all the data for now

        # Extract the last refreshed date (today's date according to the API)
        last_ref = raw.get("Meta Data", {}).get("6. Last Refreshed", "")
        date_key = last_ref.split(" ")[0]  # Remove the hours, minutes, seconds and just get the date

        # Get today's payload
        quotes = raw.get("Time Series (Digital Currency Daily)", {})
        today_quote = quotes.get(date_key, {})

        if not today_quote:
            log.warning(f"No series data for {symbol} on {date_key}")
        return today_quote, date_key

    except Exception as e:
        log.error(f"Failed fetching crypto data for {symbol}: {e}")
        return {}, ""  # Return an empty list in case an error was caught

# -----

# Pushing the crypto quotes dictionaries to the broker
async def publish_crypto_quote(symbol, name, sector, topic, producer, session):

    log.info(f"Fetching crypto {symbol} ({name}) in {sector}")
    quote, date_key = await batch_crypto_quote(symbol, session)

    if not quote:
        return

    # Clean the strings into a OHLCV dictionary
    quote = {
        "open": float(quote["1. open"]),
        "high": float(quote["2. high"]),
        "low": float(quote["3. low"]),
        "close": float(quote["4. close"]),
        "volume": float(quote["5. volume"])
    }
    
    # Attaching extra metadata to more easilty filter and search through
    message = {
        "symbol": symbol,
        "name": name,
        "sector": sector,
        "market_date": date_key,
        "crypto_info": quote
    }

    log.info(f"Sent {symbol} to {topic}")

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
