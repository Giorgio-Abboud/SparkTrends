import os, logging
from datetime import datetime, timezone
from dotenv import load_dotenv
import websocket

load_dotenv()  # Later change to dynamically pick the correct environment

# -----

# Setup logging to print errors if message sending fails
logging.basicConfig(level=logging.INFO)  # Logs messages with a level of INFO or higher (ignores DEBUG)
log = logging.getLogger(__name__)  # Retrieves logger instance specific to the current module

# Load the FinnHub API key and the Kafka Broker from the env
FINN_API_KEY = os.environ["FINN_API_KEY"]

# -----

# Fetch the stock market values that contain a specific symbol
async def batch_stock_quote(symbol, session):
    # Build the url to make the correct API call
    url = f"https://finnhub.io/api/v1/quote?symbol={symbol}&token={FINN_API_KEY}"

    # Error handling in case of a bad GET request
    try:
        async with session.get(url) as response:
            if response.status != 200:  # Ensuring that the request was successful
                log.error(f"Unsuccessful response for {symbol}: {response.status}")
                return {}
            
            return await response.json()

    except Exception as e:
        log.error(f"Failed HTTP for {symbol}: {e}")
        return {}  # Return an empty list in case an error was caught

# -----

# Pushing the stock quotes dictionaries to the broker
async def publish_stock_quote(symbol, name, sector, industry, topic, producer, session):

    log.info(f"Fetching stocks for {symbol} ({name}) in the {sector} sector")
    quote = await batch_stock_quote(symbol, session)

    if not quote:  # If nothing was received due to an error
        log.warning(f"No stocks for {symbol}")
        return

    market_date = str(datetime.fromtimestamp(quote["t"], tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S"))

    # Clean the strings into a OHLC dictionary
    quote = {
        "open": float(quote["o"]),
        "high": float(quote["h"]),
        "low": float(quote["l"]),
        "close": float(quote["c"]),
        "prev_close": float(quote["pc"]),
        "market_date": market_date
    }

    # Attaching extra metadata to more easilty filter and search through
    message = {
        "symbol": symbol,
        "name": name,
        "sector": sector,
        "industry": industry,
        "stock_info": quote
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
    log.error("Failed to send message to Kafka: ", exc_info=excp)
