import os, requests, logging
from pyspark.sql.streaming import StreamingQuery
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
    # Normalize the column names
    curr = bar_df.select("symbol", "timestamp", "close", "volume")

    # Only pull the previous 4 bars per symbol (curr + these 4)
    prevFour = (
        spark.read
            .format("jdbc")
            .option("url", config["jdbc"]["url"])
            .option("driver", config["jdbc"]["driver"])
            .option(
                "dbtable",
                """
                (SELECT *
                 FROM (
                     SELECT *,
                         ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY timestamp DESC) as rn
                     FROM stock_bars
                 ) sub
                 WHERE rn <= 4
                ) as last_bars
                """
            )
            .load()
    )

    # Union the current bar + the last four bars
    bars = prevFour.unionByName(curr)

    # Create a window to calculate the VWAP on a 5 minute window
    w = (Window.partitionBy("symbol")
               .orderBy("timestamp")
               .rowsBetween(-4, 0)
    )

    # Calculate the rolling metrics
    vw_num = F.sum(F.col("close") * F.col("volume")).over(w)  # Numerator
    vw_den = F.sum("volume").over(w)                          # Denominator
    vwap_5 = F.when(vw_den > 0, vw_num / vw_den)              # Volume-Weighted Average Price

    ret = (F.col("close") / F.lag("close", 1).over(w)) - 1    # Calculate returns
    vol_5 = F.stddev(ret).over(w)                             # Volatility over the window

    comp_metrics = (
        bars
            .withColumn("vwap_5", vwap_5)
            .withColumn("vol_5", vol_5)
    )

    # Gather the most recent rows per symbol for the anomlay detection
    newest_sym = Window.partitionBy("symbol").orderBy(F.col("timestamp").desc())

    # Drop the older bars
    metrics = (
        comp_metrics
            .withColumn("rn", F.row_number().over(newest_sym))
            .filter(F.col("rn") == 1)
            .drop("rn")
    )

    # Robust anomaly detection
    metrics = metrics.withColumn(
        "is_anomaly",
        (F.col("vwap_5").isNotNull()) &
        (F.col("vol_5").isNotNull()) &
        (F.col("vol_5") > 0) &
        (F.abs(F.col("close") - F.col("vwap_5")) > ANOMALY_THRESHOLD * F.col("vol_5"))
    )

    return metrics


# Try to find anomalies and call on slack
def detect_anomalies(metrics: DataFrame) -> None:
    # See if any anomalies pop up
    anomalies = metrics.filter(F.col("is_anomaly")).limit(50)

    # Check if there's anything
    rows = anomalies.collect()
    if not rows:
        return

    # Send alerts
    lines = []
    for r in rows:
        # guard against None just in case
        c = r['close'] or 0.0
        v = r['vwap_5'] or 0.0
        sd = r['vol_5'] or 0.0
        lines.append(
            f"Symbol: {r['symbol']}  Time: {r['timestamp']}  Close: {c:.2f}  VWAP(5): {v:.2f}  Vol_5: {sd:.4f}"
        )

    send_slack_alert("ANOMALY DETECTED\n" + "\n".join(lines))


# Streaming job ingesting raw trade data, aggregating it in 1 minute bars, and process each batch
# TRY TO DEDUPLICATE THIS LATER IN CASE OF STREAMING ERRORS
def run_minute_stream_metric(spark: SparkSession, topic: str) -> StreamingQuery:
    # Consume raw trade data from the stream
    raw_trades = (
        spark.readStream
             .format("kafka")
             .option("kafka.bootstrap.servers", BOOTSTRAP_SERVER)
             .option("subscribe", topic)
             .option("startingOffsets", "earliest")
             .option("failOnDataLoss", "false")
             .load()
             .selectExpr("CAST(value AS STRING) AS json")
             .select(
                 F.from_json(
                     F.col("json"),
                     StructType([StructField("data", ArrayType(trade_schema), True)])
                 ).alias("d")  # Store each trade in an array for later aggregation
             )
             .selectExpr("inline(d.data)")       # Pull trade data out of the subquery
             .select(
                 F.col("s").alias("symbol"),
                 F.col("p").alias("price"),
                 F.col("v").alias("volume"),
                 (F.col("t") / 1000).cast(TimestampType()).alias("timestamp")
             )
             .filter(F.col("symbol").isNotNull() & F.col("timestamp").isNotNull())
    )

    # Add a watermark to account for late incoming data
    trade_wm = raw_trades.withWatermark("timestamp", "30 seconds")

    # Aggregate the raw trades into 
    minute_bar = (
        trade_wm
            # Group by the symbols in a 1 minute window
            .groupBy(
                F.window(F.col("timestamp"), "1 minute"),
                F.col("symbol")
            )
            # Aggregate them to add the OHLCV values
            .agg(
                F.min_by(F.col("price"), F.col("timestamp")).alias("open"),
                F.max("price").alias("high"),
                F.min("price").alias("low"),
                F.max_by(F.col("price"), F.col("timestamp")).alias("close"),
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

    # Start the stream and process each batch after every minute
    query = (
        minute_bar
            .writeStream                                # Beginning to define how to stream data
            .outputMode("append")                       # Make sure not to overwrite data
            .trigger(processingTime="10 seconds")       # Process each batch at the designated time interval
            .foreachBatch(process_minute_batch)         # Manually process every static Data Frame received
            .option(                                    # Needed to remember which Kafka offsets were processed
                "checkpointLocation",                   # Also helps pickup a job again if it crashed or restarted
                f"/checkpoints/minute_metrics/{topic}"
            )
            .start()
    )

    # Return the streaming query for clean termination
    return query
