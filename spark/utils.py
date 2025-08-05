import yaml, logging
from dotenv import load_dotenv
from pyspark.sql import SparkSession, DataFrame

# Setup a logger
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# Load the .env environment
load_dotenv()

# Also create a global variable for config
with open("config.yml", "r") as f:
    config = yaml.safe_load(f)

# Create the Spark Session based on the config file
def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
            .master(config["spark"]["master"])
            .appName(config["spark"]["app_name"])
            .getOrCreate()
    )

# Generic writer function
def write_df_to_table(df: DataFrame, mode: str, table: str) -> None:
    # Write a DataFrame to a JDBC table.
    jdbc_url = config["jdbc"]["url"]
    driver = config["jdbc"]["driver"]
    log.info(f"Writing DataFrame to {table} (mode={mode})")
    (
        df.write
          .mode(mode)
          .format("jdbc")
          .option("url", jdbc_url)
          .option("dbtable", table)
          .option("driver", driver)
          .save()
    )
    log.info(f"Successfully wrote DataFrame to {table}")

# ----- Functions passing the correct values to the database writing function -----
# Writing company data
def write_company_data(df: DataFrame, mode: str) -> None:
    # Persist company metadata to company_data table
    # Expects columns: symbol, name, sector, industry
    write_df_to_table(df, mode, table="company_data")

# Writing initial stock bars
def write_stock_bars(df: DataFrame, mode: str) -> None:
    # Persist raw OHLCV bars to stock_bars table.
    # Expects columns: symbol, open, high, low, close, volume, timestamp
    write_df_to_table(df, mode, table="stock_bars")

# Writing computed stock metrics
def write_stock_metrics(df: DataFrame, mode: str) -> None:
    # Persist computed metrics (e.g., vwap, vol_5m) to stock_metrics table.
    # Expects columns: symbol, vwap, vol_5m, timestamp
    write_df_to_table(df, mode, table="stock_metrics")


