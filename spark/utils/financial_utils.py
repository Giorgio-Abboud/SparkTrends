from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, DoubleType, StringType


# Create a schema for the open, high, low, close data for stocks and crypto
# Creating a schema to ensure the data is manually structured after being extracted from JSONB format
# This tells Spark how to parse the JSON under "stock_info" or "crypto_info".
stock_schema = StructType([
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("close", DoubleType(), True),
    StructField("prev_close", DoubleType(), True),
])

crypto_schema = StructType([
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("close", DoubleType(), True),
    StructField("volume", DoubleType(), True),
])

# Reads from Kafka and returns a flattened DataFrame with the correct columns
def read_and_flatten(spark, entity, kafka_bootstrap_servers):
    # Select the correct column fields based on the entity provided
    if entity == "stock":
        topic = "current_stock"  # Topic name
        info = "stock_info"      # JSON key for the OHLCP schema
        schema = stock_schema

        # Define the nested columns
        nested_cols = [
            F.col(f"msg.{info}.open").alias("open"),
            F.col(f"msg.{info}.high").alias("high"),
            F.col(f"msg.{info}.low").alias("low"),
            F.col(f"msg.{info}.close").alias("close"),
            F.col(f"msg.{info}.prev_close").alias("prev_close"),
        ]

    else:
        topic = "current_crypto"  # Topic name
        info = "crypto_info"      # JSON key for the OHLCV schema
        schema = crypto_schema

        # Define the nested columns
        nested_cols = [
            F.col(f"msg.{info}.open").alias("open"),
            F.col(f"msg.{info}.high").alias("high"),
            F.col(f"msg.{info}.low").alias("low"),
            F.col(f"msg.{info}.close").alias("close"),
            F.col(f"msg.{info}.volume").alias("volume"),
        ]

    # Build the full schema consiting of top level and nested fields
    full_schema = StructType([
        StructField("symbol", StringType(), True),
        StructField("name", StringType(), True),
        StructField("sector", StringType(), True),
        StructField("market_date", StringType(), True),
        StructField(info, schema, True),
    ])

    # Read the incoming data, parse it, and flatten all in one go
    df = (
        spark.readStream
            # Start by connecting to kafka
            .format("kafka")
            .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
            .option("subscribe", topic)
            .option("startingOffsets", "latest")  # Get the latest first
            .option("failOnDataLoss", False)      # Keep running if a message or offset is removed
            .load()
            # Convert raw Kafka bytes into a JSON string column
            .selectExpr("CAST(value AS STRING) AS raw_json")
            # Parse the JSON string into a struct named msg using the full_schema
            .select(F.from_json(F.col("raw_json"), full_schema).alias("msg"))
            # Flatten all the fields from the struct
            .select(
                # Surface level fields
                "msg.symbol",
                "msg.name",
                "msg.sector",
                # Comvert the market date string in fromat yyyy-MM-dd to a DateTime column
                F.to_date(F.col("msg.market_date"), "yyyy-MM-dd").alias("market_date"),
                # Nested fields open, high, low, close, volume or prev_close from the nested struct
                *nested_cols
            )
    )

    # Neutral value for volume
    return df.withColumn("volume", F.lit(0.0))


# Calculating the stock and crypto returns
def compute_returns(df, entity):
    # Add a column to the data frame with that day's returns
    return df.withColumn(f"{entity}_returns", (F.col("close") - F.col("open")) / F.col("open"))


# Calculate the stock and crypto volatility
def compute_volatility(df, entity, days=7):
    # Compute the rolling volatility with a default of 7 days if days is not specified
    w = (
        Window.partitionBy("symbol")   # Partition by either stock or crypto
            .orderBy("market_date")    # Keep it organized by market time
            .rowBetween(-days + 1, 0)  # rows with data from today to n - 1 days ealier (-1 account for today)
    )

    # Calculate the standard deviation of the returns over the last n days using our window function
    return df.withColumn(f"{entity}_volatility", F.stddev(f"{entity}_returns").over(w))
