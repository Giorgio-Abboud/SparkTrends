from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType, StringType


# Create a schema for the open, high, low, close data for stocks and crypto
# Creating a schema to ensure the data is manually structured after being extracted from JSONB format
# This tells Spark how to parse the JSON under "stock_info" or "crypto_info".
ohlc_schema = StructType([
    StructField("open", DoubleType()),
    StructField("high", DoubleType()),
    StructField("low", DoubleType()),
    StructField("close", DoubleType()),
    StructField("volume", IntegerType()),
])


# Reads from Kafka and returns a flattened DataFrame with the correct columns
# ticker / company / sector (for stocks)
# symbol / crypto / category (for crypto)
def read_and_flatten(spark, entity, kafka_bootstrap_servers):
    # Depending on the entity select the correct columns
    # The correct column names can be found in the producer files
    if entity == "stock":
        topic = "current_stock"  # Topic name
        info = "stock_info"      # JSON key for the OHLCV schema
        # The surface level fields we can easily extract
        surface_fields = [
            StructField("ticker", StringType()),   # Stock symbol
            StructField("company", StringType()),  # Company name
            StructField("sector", StringType()),   # Industry sector
        ]
    
    else:
        topic = "current_crypto"  # Topic name
        info = "crypto_info"      # JSON key for the OHLCV schema
        # The surface level fields we can easily extract
        surface_fields = [
            StructField("symbol", StringType()),     # Crypto symbol
            StructField("crypto", StringType()),     # Crypto name
            StructField("category", StringType()),   # Crypto category
        ]

    # Add the incoming market date of type string into the schema
    surface_fields.append(StructField("market_date", StringType()))

    # Build the full schema consiting of top level and nested fields
    full_schema = StructType(surface_fields + [StructField(info, ohlc_schema)])

    # Read the incoming data, parse it, and flatten all in one go
    return (
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
                F.col(f"msg.{surface_fields[0].name}").alias(surface_fields[0].name),  # ticker / symbol
                F.col(f"msg.{surface_fields[1].name}").alias(surface_fields[1].name),  # company / crypto
                F.col(f"msg.{surface_fields[2].name}").alias(surface_fields[2].name),  # sector / category

                # Comvert the market date string in fromat yyyy-MM-dd to a DateTime column
                F.to_date(F.col("msg.market_date"), "yyyy-MM-dd").alias("market_date"),

                # Nested fields open, high, low, close, volume from the nested struct
                F.col(f"msg.{info}.open").alias("open"),
                F.col(f"msg.{info}.high").alias("high"),
                F.col(f"msg.{info}.low").alias("low"),
                F.col(f"msg.{info}.close").alias("close"),
                F.col(f"msg.{info}.volume").alias("volume"),
            )
    )


# Calculating the stock and crypto returns
def compute_returns(df, entity):
    # Add a column to the data frame with that day's returns
    return df.withColumn(f"{entity}_returns", (F.col("close") - F.col("open")) / F.col("open"))


# Calculate the stock and crypto volatility
def compute_volatility(df, entity, days=7):
    # Compute the rolling volatility with a default of 7 days if days is not specifiedsym_col = "ticker" if entity == "stock" else "symbol"
    sym_col = "ticker" if entity == "stock" else "symbol"

    w = (
        Window.partitionBy(sym_col)   # Partition by either stock or crypto
            .orderBy("market_date")    # Keep it organized by market time
            .rowBetween(-days + 1, 0)  # rows with data from today to n - 1 days ealier (-1 account for today)
    )

    # Calculate the standard deviation of the returns over the last n days using our window function
    return df.withColumn(f"{entity}_volatility", F.stddev(f"{entity}_returns").over(w))
