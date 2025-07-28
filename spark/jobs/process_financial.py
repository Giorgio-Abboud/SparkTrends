import os
import yaml
import logging
import argparse
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from utils.financial_utils import read_and_flatten, compute_returns, compute_volatility
from utils.prediction_utils import train_model, predict_returns

# Setup a logger
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# Load the .env environment
load_dotenv()

# Also create a global variable for config
with open("config.yml", "r") as f:
    config = yaml.safe_load(f)

# Create the Spark Session that will be passed into the function calls
def create_spark_session():
    return (
        SparkSession.builder
            .master(config["spark"]["master"])
            .appName(config["spark"]["app_name"])
            .getOrCreate()
    )


# Run a continuous streaming ETL that listens to Kafka and processes in micro batches until stopped
def run_streaming_etl(spark):
    # Retrieve the jdbc url, the model path, horizon days, and window frame from the config file
    jdbc_url = config["jdbc"]["url"]
    model_path = config["model"]["model_path"]
    horizon = config["window"]["horizon_days"]
    days = config["window"]["weekly_volatility"]
    kafka_bootstrap_servers = config["kafka"]["kafka_bootstrap_servers"]

    # Store the current streaming queries to later terminate (will continue running until terminated)
    queries = []

    # Process the stock and crypto topics
    for entity in ["stock", "crypto"]:
        # Creating a streaming Data Frame for stock and crypto
        stream_df = read_and_flatten(spark, entity, kafka_bootstrap_servers)

        # Define the micro batch processing logic
        def micro_batch_processing(batch_df, batch_id):
            # Make sure a trained model exists first
            if not os.path.exists(model_path):
                log.warning(f"No model found at {model_path}, skipping predictions")
                return

            # Compute the returns
            ret_df = compute_returns(batch_df, entity)

            # Compute volatility
            vol_df = compute_volatility(ret_df, entity, days)

            # Compute the predicted close, return and volatility
            predict_returns(vol_df, entity, jdbc_url, model_path, horizon, days)

        # Start a streaming query that continuously pulls from Kafka
        query = (
            stream_df
                .writeStream                           # Beginning to define how to stream data
                .forEachBatch(micro_batch_processing)  # Manually process every static Data Frame received
                .option(                               # Needed to remember which Kafka offsets were processed
                    "checkpointLocation",              # Also helps pickup a job again if it crashed or restarted
                    f"/checkpoints/{entity}"
                    )
                .start()                               # Run it
        )

        # Add this query to the list defined above
        queries.append(query)

    # Await for stream query termination
    spark.streams.awaitAnyTermination()


# Train the machine learning model and save it
def run_ml_pipeline(spark):
    jdbc_url = config["jdbc"]["url"]
    model_path = config["model"]["model_path"]
    csv_path = config["model"]["csv_path"]
    horizon = config["window"]["horizon_days"]
    test_decimal = config["model"]["test_dec"]
    seed = config["model"]["seed"]

    # Pass the metrics to the function
    train_model(spark, csv_path, model_path, seed, jdbc_url, horizon, test_decimal)


# Command line interface entry point
def main():
    # Define our parser
    parser = argparse.ArgumentParser(description="SparkTrends: Streaming ETL and ML Training")

    # Subparser to create the ETL and train subcommands
    subparsers = parser.add_subparsers(dest="mode", required=True, help="Mode: Stream ETL or Train ML")

    # ETL Streaming Mode
    subparsers.add_parser("stream", help="Run streaming ETL and prediction")

    # ML Training Mode
    subparsers.add_parser("train", help="Train the GBT model on historical CSV data")

    # Get the arguments passed into parser
    args = parser.parse_args()

    # Create a Spark session
    spark = create_spark_session()

    # Run the ETL streaming mode
    if args.mode == "stream":
        run_streaming_etl(spark)

    # Run the ML model training mode
    elif args.mode == "train":
        run_ml_pipeline(spark)


if __name__ == "__main__":
    main()