import logging
from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from spark.utilities import write_stock_bars, write_company_data
from dotenv import load_dotenv
import os

# Setup logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

load_dotenv()
BOOTSTRAP_SERVER = os.environ["KAFKA_BROKER"]

# JSON OHLCV batch data schema
ohlcv_schema = StructType([
    StructField("symbol", StringType(), False),
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("close", DoubleType(), True),
    StructField("volume", DoubleType(), True),
    StructField("timestamp", TimestampType(), False)
])

# JSON company meta data schema
meta_schema = StructType([
    StructField("symbol", StringType(), False),
    StructField("name", StringType(), True),
    StructField("sector", StringType(), True),
    StructField("industry", StringType(), True)
])

def read_kafka(spark: SparkSession, topic: str, schema: StructType) -> DataFrame:
    log.info(f"Reading from Kafka topic '{topic}' using bootstrap server '{BOOTSTRAP_SERVER}'")
    df = (
        spark.read
             .format("kafka")
             .option("kafka.bootstrap.servers", BOOTSTRAP_SERVER)
             .option("subscribe", topic)
             .option("startingOffsets", "earliest")
             .option("endingOffsets", "latest")
             .option("failOnDataLoss", "false")
             .load()
             .selectExpr("CAST(value AS STRING) AS json")
             .select(F.from_json(F.col("json"), schema).alias("data"))
             .select("data.*")
    )
    log.info(f"Read DataFrame from topic '{topic}'. Row count: {df.count()}")
    df.show(5, truncate=False)
    return df

def load_batch_ohlcv(spark: SparkSession, topic: str) -> None:
    log.info(f"Starting batch OHLCV load for topic '{topic}'")
    ohlcv_df = read_kafka(spark, topic, ohlcv_schema)
    log.info(f"Writing OHLCV data to DB. Row count: {ohlcv_df.count()}")
    try:
        write_stock_bars(ohlcv_df, "append")
        log.info("Successfully wrote OHLCV data to Postgres")
    except Exception as e:
        log.error(f"Failed to write OHLCV data to Postgres: {e}")

def load_batch_meta(spark: SparkSession, topic: str) -> None:
    log.info(f"Starting batch meta-data load for topic '{topic}'")
    meta_df = read_kafka(spark, topic, meta_schema)
    log.info(f"Writing meta-data to DB. Row count: {meta_df.count()}")
    try:
        write_company_data(meta_df, "overwrite")
        log.info("Successfully wrote meta-data to Postgres")
    except Exception as e:
        log.error(f"Failed to write meta-data to Postgres: {e}")
