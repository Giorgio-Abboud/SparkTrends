import argparse, asyncio, csv, aiohttp, logging
from apache_kafka.producers.stock_producer import stream_stock, batch_stock, stock_meta
from apache_kafka.utilities import connect_to_kafka, create_topics
from kafka.admin import NewTopic
from spark.utilities import create_spark_session
from spark.jobs.stock_batch import load_batch_ohlcv, load_batch_meta
from spark.jobs.stock_stream import run_minute_stream_metric
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

load_dotenv()

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
    producer = await connect_to_kafka(6, 5)
    spark = create_spark_session()

    spark.sparkContext.setLogLevel("WARN")  # Hide most logs

    # Create the topics with retry logic
    create_topics([
        NewTopic("stream_stock", 3, 1),
        NewTopic("batch_stock", 3, 1),
        NewTopic("stock_meta", 3, 1)
    ])

    symbols = load_symbols(args.file)

    log.info("Beginning {args.mode} mode")

    # If we decide to stream
    if args.mode == "stream":
        # 1) start the websocket→kafka stream task
        stock_task = asyncio.create_task(
            stream_stock(symbols, producer, "stream_stock"),
            name="stream_stock"
        )

        # 2) start the Spark query (make sure your run_minute_stream_metric calls .start())
        stock_query = run_minute_stream_metric(spark, "stream_stock")

        # 3) await either: a) Spark termination, or b) stream task failure
        spark_task = asyncio.create_task(
            asyncio.to_thread(spark.streams.awaitAnyTermination),
            name="spark_await"
        )
        try:
            done, pending = await asyncio.wait(
                {stock_task, spark_task},
                return_when=asyncio.FIRST_EXCEPTION  # finishes if either errors/terminates
            )

            # if stream task died, log its exception and stop Spark
            if stock_task in done and stock_task.exception():
                exc = stock_task.exception()
                log.error("stream_stock crashed", exc_info=exc)
                if stock_query.isActive():
                    stock_query.stop()

            # if Spark finished first, we’re done; fall through to cleanup
        finally:
            # cancel whatever’s still running
            for t in (stock_task, spark_task):
                if not t.done():
                    t.cancel()
            await asyncio.gather(stock_task, spark_task, return_exceptions=True)

            # shutdown Spark & Kafka cleanly
            if stock_query.isActive():
                stock_query.stop()
            spark.stop()
            await producer.stop()


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
