import argparse, asyncio, csv, aiohttp, logging
from kafka.producers.stock_producer import stream_stock, batch_stock, stock_meta
from kafka.utils import connect_to_kafka, create_topics
from kafka.admin import NewTopic
from spark.utils import create_spark_session
from spark.jobs.stock_batch import load_batch_ohlcv, load_batch_meta
from spark.jobs.stock_stream import run_minute_stream_metric

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


# Track all of the symbols in our CSV file
def load_symbols(path):
    with open(path, "r", newline="", encoding="utf-8") as f:
        return [row["Symbol"] for row in csv.DictReader(f)]


async def main():
    # Setup argparse CLI
    p = argparse.ArgumentParser()

    # Our two modes of either streaming FinnHub or Batch from Twelve Data
    p.add_argument("--mode", choices=["stream", "batch", "meta"], required=True)
    # If you want to change the file
    p.add_argument("--file", default="tracked_data/important_top50.csv")
    # To alter the rate of harvesting API data
    p.add_argument("--rate", type=float, default=60/8)
    # Choose if we want to append of overwrite data
    p.add_argument("--dbmode", choices=["append", "overwrite"], default="append")

    # Get the selected arguments
    args  = p.parse_args()

    # Create a producer
    producer = await connect_to_kafka(5, 5)
    spark = create_spark_session()

    # Create the topics with retry logic
    create_topics([
        NewTopic("stream_stock", 3, 1),
        NewTopic("batch_stock", 3, 1),
        NewTopic("stock_meta", 3, 1)
    ])

    symbols = load_symbols(args.file)

    # If we decide to stream
    if args.mode == "stream":
        await stream_stock(symbols, producer, "stream_stock")
        run_minute_stream_metric(spark, "stream_stock")

    # If we get batch data
    elif args.mode == "batch":
        async with aiohttp.ClientSession() as session:
            await batch_stock(symbols, producer, session, "batch_stock", args.rate)
            load_batch_ohlcv(spark, "batch_stock")

    # If we want to find meta data like company symbol, name, sector and industry
    elif args.mode == "meta":
        await stock_meta(producer, args.file, "stock_meta")
        load_batch_meta(spark, "stock_meta")

    await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())
