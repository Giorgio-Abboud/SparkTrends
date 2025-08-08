import yaml, logging
from dotenv import load_dotenv
from pyspark.sql import SparkSession, DataFrame

# Setup a logger
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# Load the .env environment
load_dotenv()

# Load config
with open("spark/config.yml", "r") as f:
    config = yaml.safe_load(f)

# Create the Spark Session based on the config file
def create_spark_session() -> SparkSession:
    log.info("Creating Spark session...")
    session = (
        SparkSession.builder
            .master(config["spark"]["master"])
            .appName(config["spark"]["app_name"])
            .getOrCreate()
    )
    log.info("Spark session created.")
    return session

# Generic writer function
def write_df_to_table(df: DataFrame, mode: str, table: str) -> None:
    jdbc_url = config["jdbc"]["url"]
    driver = config["jdbc"]["driver"]
    row_count = df.count()
    log.info(f"Writing {row_count} rows to {table} (mode={mode})")
    df.show(5, truncate=False)
    try:
        (
            df.write
              .mode(mode)
              .format("jdbc")
              .option("url", jdbc_url)
              .option("dbtable", table)
              .option("driver", driver)
              .save()
        )
        log.info(f"Successfully wrote {row_count} rows to {table}")
    except Exception as e:
        log.error(f"Failed to write DataFrame to {table}: {e}")

# ----- Functions passing the correct values to the database writing function -----
def write_company_data(df: DataFrame, mode: str) -> None:
    log.info("Writing company meta-data to company_data table")
    write_df_to_table(df, mode, table="company_data")

def write_stock_bars(df: DataFrame, mode: str) -> None:
    log.info("Writing OHLCV stock bars to stock_bars table")
    write_df_to_table(df, mode, table="stock_bars")

def write_stock_metrics(df: DataFrame, mode: str) -> None:
    log.info("Writing computed metrics to stock_metrics table")
    write_df_to_table(df, mode, table="stock_metrics")
