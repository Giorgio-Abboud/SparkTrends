import os, logging, csv, json, aiohttp, asyncio
from aiokafka import AIOKafkaProducer
from datetime import datetime, timezone
from dotenv import load_dotenv
from websockets import connect
from zoneinfo import ZoneInfo

load_dotenv()  # Later change to dynamically pick the correct environment

# -----

# Setup logging to print errors if message sending fails
logging.basicConfig(level=logging.INFO)  # Logs messages with a level of INFO or higher (ignores DEBUG)
log = logging.getLogger(__name__)  # Retrieves logger instance specific to the current module

# Load the FinnHub and Twelve Data API keys
FINN_API_KEY = os.environ["FINN_API_KEY"]
TWELVE_API_KEY = os.environ["TWELVE_API_KEY"]
TWELVE_URL = "https://api.twelvedata.com/time_series"

# -----

# Subscribe to Finnhub WebSocket for a list of symbols and publish each tick
async def stream_stock(symbols: list[str], producer: AIOKafkaProducer, topic: str):
    # Connect to the websocket
    async with connect(f"wss://ws.finnhub.io?token={FINN_API_KEY}") as ws:
        # Subscribe to all symbols in the list
        for sym in symbols:
            await ws.send(json.dumps({"type": "subscribe", "symbol": sym}))
            log.info(f"Subscribed to {sym}")

        # Send the data received to the Kafka broker
        async for msg in ws:
            payload = json.load(msg)
            # Store the trades in a list and iterate
            for trade in payload.get("data", []):
                # Fix the timestamp (in milisec so divide by 1k)
                ts = datetime.fromtimestamp(trade["t"] / 1000, tz=timezone.utc)
                # Frame the record in a dictionary
                record = {
                    "symbol": trade["s"],
                    "price": trade["p"],
                    "volume": trade["v"],
                    "timestamp": ts.isoformat()
                }
                # Try sending it to the broker
                try:
                    await producer.send_and_wait(topic, record)
                    log.info(f"Streamed {record['symbol']} at {record['ts']}")
                except Exception as e:
                    log.error(f"Failed to stream tick for {record['symbol']}: {e}")

# -----

# Fetch OHLCV data from Twelve Data API for the passed symbols
async def fetch_ohlcv_td(symbol: str, session: aiohttp.ClientSession) -> dict:
    # Fetch 1-min OHLCV bars from Twelve Data
    params = {
        "symbol": symbol,
        "interval": "1min",
        "outputsize": 25,
        "apikey": TWELVE_API_KEY
    }
    # Return an output to batch_stock
    async with session.get(TWELVE_URL, params=params) as response:
        response.raise_for_status()
        return await response.json()
    
# Fetch the stock data from fetch_ohlcv_td, format and send to the Kafka broker
async def batch_stock(symbols: list[str], producer: AIOKafkaProducer,
                      session: aiohttp.ClientSession, topic: str, rate: float):
    # Call Twelve Data using the provided symbols
    for sym in symbols:
        try:
            # Call and extract the stock data
            data = await fetch_ohlcv_td(sym, session)
            quotes = data.get("values", [])

            # For loop in case we want to increase the output size later on
            for q in quotes:
                # Fix the timestamp to UTC
                dt_naive = datetime.strptime(q["datetime"], "%Y-%m-%d %H:%M:%S")
                dt_utc = dt_naive.replace(tzinfo=ZoneInfo("America/New_York")).astimezone(timezone.utc)
                # Create a record out of the values
                record = {
                    "symbol": sym,
                    "open": float(q["open"]),
                    "high": float(q["high"]),
                    "low": float(q["low"]),
                    "close": float(q["close"]),
                    "volume": float(q["volume"]),
                    "timestamp": dt_utc.isoformat()
                }
                # Send to the producer
                await producer.send_and_wait(topic, record)
            log.info(f"Batch fetch of {len(quotes)} quotes complete for {sym}")

        except Exception as e:
            log.error(f"Batch failed for {sym}: {e}")

        # Respect Twelve Data's 8 API request per minute limit
        await asyncio.sleep(rate)

# -----

# Read CSV of company metadata and publish each row to Kafka for downstream joins
async def stock_meta(producer: AIOKafkaProducer, file: str, topic: str):
    with open(file, "r", newline="", encoding="utf-8") as f:
        # Create the reader object
        reader = csv.DictReader(f)
        # Go through each row to gather the stock meta data
        for row in reader:
            try:
                # Store a dict full of the values with the correct column name and send
                record = {
                    "symbol": row["Symbol"],
                    "name": row["Name"],
                    "sector": row["Sector"],
                    "industry": row["Industry"]
                }
                await producer.send_and_wait(topic, record)
                log.info(f"Published metadata for {record['symbol']}")

            except Exception as e:
                log.error(f"Failed to publish metadata for {record['symbol']}: {e}")
