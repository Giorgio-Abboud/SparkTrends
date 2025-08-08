import os, requests, logging
from pyspark.sql import SparkSession, DataFrame, Window, functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, ArrayType
from spark.utilities import write_stock_bars, write_stock_metrics, config

from dotenv import load_dotenv
from pyspark.sql import SparkSession

# Setup a logger
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# Load the .env environment
load_dotenv()

# Load the Slack alert settings
ANOMALY_THRESHOLD = 3.0
SLACK_WEBHOOK = os.environ["SLACK_WEBHOOK"]
BOOTSTRAP_SERVER = os.environ["KAFKA_BROKER"]

# Schema for individual trade messages from Kafka
trade_schema = StructType([
    StructField("p", DoubleType(), False),  # Trade price
    StructField("s", StringType(), False),  # Stock symbol
    StructField("t", DoubleType(), False),  # Epoch ms timestamp
    StructField("v", DoubleType(), False),  # Trade volume
])


# Send notifications to slack if any anomalies are detected
def send_slack_alert(message: str) -> None:
    response = requests.post(SLACK_WEBHOOK, json={"text": message})
    response.raise_for_status()


# Process the computation to detect anomalies
def process_computation(spark: SparkSession, bar_df: DataFrame) -> DataFrame:
    # Start by pulling 10 bars (more than we need) from the bars table and for a specific symbol
    bars = (
        spark.read
            .format("jdbc")
            .option("url", config["jdbc"]["url"])
            .option(
                "dbtable",
                """
                (SELECT *
                 FROM (
                     SELECT *,
                         ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY timestamp DESC) as rn
                     FROM stock_bars
                 ) sub
                 WHERE rn <= 10
                ) as last_bars
                """
            )
            .option("driver", config["jdbc"]["driver"])
            .load()
    )

    # Create a window to calculate the VWAP on a 5 minute window
    w = (Window.partitionBy("symbol")
               .orderBy("timestamp")
               .rowsBetween(-4, 0)
    )

    # Calculate the 5 min VWAP, volatility, and check for anomalies
    metrics = (
        bars
            .withColumn(
                "vwap_5",
                F.sum(F.col("close") * F.col("volume")).over(w) /
                F.sum("volume").over(w)
            )
            .withColumn(
                "vol_5",
                F.stddev("close").over(w)
            )
            .withColumn(
                "is_anomaly",
                F.abs(F.col("close") - F.col("vwap_5")) > ANOMALY_THRESHOLD * F.col("vol_5")
            )
    )

    # Gather all recent messages to check for anomalies
    latest_ts_per_symbol = (
        bar_df.groupBy("symbol")
              .agg(F.max("timestamp").alias("latest"))
    )

    new_metrics = (
        metrics.join(
            latest_ts_per_symbol,
            (metrics.symbol == latest_ts_per_symbol.symbol) &
            (metrics.timestamp == latest_ts_per_symbol.latest),
            "inner"
        ).select(*metrics)
    )

    return new_metrics


# Try to find anomalies and call on slack
def detect_anomalies(metrics: DataFrame) -> None:
    # See if any anomalies pop up
    anomalies = metrics.filter(F.col("is_anomaly")).collect()

    # If the anomalies list is not empty then something came up
    if anomalies:
        lines = [
            f"Symbol: {r['symbol']}  Time: {r['timestamp']}  Close: {r['close']:.2f}  VWAP(5): {r['vwap_5']:.2f}  Vol_5: {r['vol_5']:.2f}"
            for r in anomalies
        ]
        message = "ANOMALY DETECTED\n" + "\n".join(lines)
        send_slack_alert(message)


# Streaming job ingesting raw trade data, aggregating it in 1 minute bars, and process each batch
# TRY TO DEDUPLICATE THIS LATER IN CASE OF STREAMING ERRORS
# ALSO ADD WATERMARKS TO SAFELY HANDLE DELAYED MESSAGES
def run_minute_stream_metric(spark: SparkSession, topic: str) -> None:
    # Consume raw trade data from the stream
    raw_trades = (
        spark.readStream
             .format("kafka")
             .option("kafka.bootstrap.servers", BOOTSTRAP_SERVER)
             .option("subscribe", topic)
             .option("startingOffsets", "latest")
             .option("failOnDataLoss", "false")
             .load()
             .selectExpr("CAST(value AS STRING) AS json")
             .select(
                 F.from_json(F.col("json"), StructType([  # Store each trade in an array for later aggregation
                     StructField("data", ArrayType(trade_schema), True)
                 ])).alias("d")
             )
             .selectExpr("inline(d.data) AS trade")       # Pull trade data out of the subquery
             .select(
                 F.col("trade.s").alias("symbol"),
                 F.col("trade.p").alias("price"),
                 F.col("trade.v").alias("volume"),
                 (F.col("trade.t") / 1000).cast(TimestampType()).alias("timestamp")
             )
    )

    # Aggregate the raw trades into 
    minute_bar = (
        raw_trades
            # Group by the symbols in a 1 minute window
            .groupBy(
                F.window(F.col("timestamp"), "1 minute"),
                F.col("symbol")
            )
            # Aggregate them to add the OHLCV values
            .agg(
                F.first("price").alias("open"),
                F.max("price").alias("high"),
                F.min("price").alias("low"),
                F.last("price").alias("close"),
                F.sum("volume").alias("volume")
            )
            .select(
                F.col("symbol"),
                F.col("window.start").alias("timestamp"),  # window struct start will be the inital timestamp
                "open", "high", "low", "close", "volume"   # Aggregation function columns
            )
    )

    # Sub function used to call computation functions
    def process_minute_batch(bar_df: DataFrame, batch_id: int) -> None:
        # Store the current bar data to PostgreSQL
        write_stock_bars(bar_df, mode="append")

        # Compute the data using the most recent bars
        metrics = process_computation(spark, bar_df)

        # Store the metrics in PostgreSQL
        write_stock_metrics(metrics, mode="append")

        # Search for anomalies
        detect_anomalies(metrics)

    # Process each batch after every minute
    (
        minute_bar
            .writeStream                         # Beginning to define how to stream data
            .trigger(processingTime="1 minute")  # Process each batch at the designated time interval
            .foreachBatch(process_minute_batch)  # Manually process every static Data Frame received
            .option(                             # Needed to remember which Kafka offsets were processed
                "checkpointLocation",            # Also helps pickup a job again if it crashed or restarted
                f"/checkpoints/minute_metrics"
            )
            .start()
    )

    # Run until told to quit
    spark.streams.awaitAnyTermination()
