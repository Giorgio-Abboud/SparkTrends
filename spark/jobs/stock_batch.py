from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from utils import write_stock_bars, write_company_data
from dotenv import load_dotenv
import os

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

# Start reading from the Kakfa broker
def read_kafka(spark: SparkSession, topic: str, schema: StructType) -> DataFrame:
    return (
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

# Load the batch data from Kakfa and send it off to be stored in PostgreSQL
def load_batch_ohlcv(spark: SparkSession, topic: str) -> None:
    ohlcv_df = read_kafka(spark, topic, ohlcv_schema)
    write_stock_bars(ohlcv_df, "append")

# Load the company meta data from Kakfa and send it off to be stored in PostgreSQL
def load_batch_meta(spark: SparkSession, topic: str) -> None:
    meta_df = read_kafka(spark, topic, meta_schema)
    write_company_data(meta_df, "overwrite")
