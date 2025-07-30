from producers import news_producer, stock_producer, crypto_producer
import os, json, csv, logging, asyncio, aiohttp
from aiokafka import AIOKafkaProducer
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
async def connect_to_kafka(max_retries=5, delay=5):
    # Tries to connect to kafka 'max_retries' amount of times
    for attempt in range(max_retries):
        try:
            log.info(f"Attempt {attempt + 1}: Connecting to Kafka")

            # Attempt to create a Kafka producer instance and raises an exception if Kafka isn't reachable
            # producer = KafkaProducer(
            #     bootstrap_servers=BOOTSTRAP_SERVER,  # Address of the Kafka broker
            #     value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serialize Python dicts to JSON bytes before sending to Kafka
            # )

            producer = AIOKafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVER,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
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

# Load the companies that will be tracked for news from AlphaVantage
def load_tracked_news(file="tracked_data/tracked_companies.json"):
    with open(file, "r") as f:
        tracker = json.load(f)
    
    # Flatten json dictionaries to ensure easier data processing and analysis
    flatten = [
        (sector, data["name"], data["symbol"])
        for sector, map in tracker.items()
        for data in map
    ]

    return flatten

# -----

# Load the crypto that will be tracked from AlphaVantage
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

# Function for batch publishing messages to the broker for finnhub data (stocks)
async def batch_publish_finn(producer, session, file="tracked_data/cleaned_symbols.csv"):

    # Read throught the csv file
    with open(file, "r", newline="", encoding="utf-8") as f:
        # Create the reader object
        reader = csv.DictReader(f)

        # Go through reach row and flatten the values into tuples
        for row in reader:
            symbol = row["Symbol"]
            name = row["Name"]
            sector = row["Sector"]
            industry = row["Industry"]

            await stock_producer.publish_stock_quote(symbol, name, sector, industry, "current_stock", producer, session)

            await asyncio.sleep(1)  # For the 60 request per minute limit (FinnHub)

# -----

# Function for batch publishing messages to the broker for alpha vantage data (crypto + news)
async def batch_publish_alpha(producer, session):
    tracked_data = []  # List to store the data for the resquests to be made
    flat_news = load_tracked_news()  # Load the flattened data for companies and stocks
    flat_crypto = load_tracked_crypto()  # Load the flattened data for crypto

    for sector, name, symbol in flat_news:  # Adding a tuple with the necessary data for news
        tracked_data.append(("news", sector, name, symbol, "current_news"))

    for sector, name, symbol in flat_crypto:  # Same for crypto
        tracked_data.append(("crypto", sector, name, symbol, "current_crypto"))

    # Call the respective producers and provide the necessary data
    for feed_type, sector, name, symbol, topic in tracked_data:

        if feed_type == "news":
            await news_producer.process_news_symbol(symbol, name, sector, topic, producer, session)

        elif feed_type == "crypto":
            await crypto_producer.process_crypto_symbol(symbol, name, sector, topic, producer, session)

        await asyncio.sleep(12)  # Make sure to get 5 requests per minute (5 request limit for AlphaVantage)

# -----

# Mark main as a coroutine function
async def main():
    # Start by creating a producer instance
    producer = await connect_to_kafka()

    # Create an aiohttp session for all batches to use
    async with aiohttp.ClientSession() as session:
        await asyncio.gather(  # Gather takes in multiple routines and runs them concurrently
            batch_publish_finn(producer, session),
            batch_publish_alpha(producer, session),
        )

    # Await to avoid getting blockd on one task
    await producer.stop()  # Shutdown the producer

    log.info("Producer stopped")

if __name__ == "__main__":
    asyncio.run(main())