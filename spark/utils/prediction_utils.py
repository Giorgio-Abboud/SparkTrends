from pyspark.sql import Row, functions as F
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline, PipelineModel
from collections import defaultdict
from datetime import datetime, timezone

# This file provides reusable Spark MLlib-based functions for training and
# applying regression models to historical OHLCV data

# This function will train a GBT (Gradient Boosted Tree) regression model on past OHLCV data
# Paremeters include the spark session, csv path, model path, the model seed
# the jdbc configs, how many days ahead, and decimal of testing split
def train_model(spark, csv_path, model_path, seed, jdbc_url, horizon, test_decimal):
    # Create a dataframe from the loaded csv data
    df = (
        spark.read
             .option("header", True)       # First row will be the column names
             .option("inferSchema", True)  # Automatically infer data type
             .csv(csv_path)
    )

    # Only select the open, high, low, close, market date and volume columns
    df = df.select("open", "high", "low", "close", "volume", "market_date")

    # Add a column containing the following day's closing price to train the model to predict 1 day ahead
    w = Window.orderBy("market_date")        # Window function to tell spark which row is next (next date)
    df = df.withColumn("next_close", F.lead("close", horizon).over(w))  # next_close is the actual next value
    df = df.dropna(subset=["next_close"])    # Drop the following rows with null values (more can be added)

    # Prepare columns into a vector column for the machine learning algorithm
    # Omit the next close data since we will be predicting the close prices
    feature_columns = ["open", "high", "low", "close", "volume"]
    assembler = VectorAssembler(
        inputCols=feature_columns,  # The list of input columns
        outputCol="features"        # The single output column it creates
    )

    # Will split the data frame into training and testing data
    train_df, test_df = df.randomSplit(
        [1 - test_decimal, test_decimal],  # A random 80% of the rows will be used for training and the other random 20% for testing
        seed = seed                        # A seed to keep the same split (can be changed if not effective enough)
    )

    # Gradient Boosted Trees are used to model non-linear patterns for the close prices
    gbt = GBTRegressor(
        featuresCol="features",      # Name of the column holding the feature vector
        labelCol="next_close",       # The name of the column we want to predict
        predictionCol="prediction",  # The name of the column Spark will output with its predictions
        maxIter=50                   # Number of trees in the ensemble
    )

    # Create a Spark ML Pipeline to bundle the transformer and estimator into a single object
    pipeline = Pipeline(stages=[assembler, gbt])  # Bundle both steps into a pipeline

    # assembler is a Transformer, so it calls assembler.transform(train_df) to add the "features" column
    # gbt is an Estimator, so it then calls gbt.fit(...) on the data frame with "features" to produce a fitted GBTModel
    model = pipeline.fit(train_df)                # Train assembler and GBT in order

    # Evaluate the model on the testing df using RMSE, MAE, and R2 metrics.
    predictions = model.transform(test_df)       # Apply the full pipeline and generate a new df

    # Dictionary to store the prediction scores for later use
    pred_score = defaultdict(float)

    for metric in ["rmse", "mae", "r2"]:         # Computing the RMSE (Root Mean Squared Error), MAE (Mean Absolute Error) and R² (Coefficient of Determination)
        evaluator = RegressionEvaluator(
            labelCol="next_close",               # Stating which column holds the true value
            predictionCol="prediction",          # Where the model's predictions exist
            metricName=metric                    # The current metric to be tested
        )
        score = evaluator.evaluate(predictions)  # Spark will scan each row and calculate how many passed
        pred_score[metric] = score
        print(f"{metric.upper()} on test data: {score:.4f}")

    # Save the trained model into the desired path
    model.write().overwrite().save(model_path)

    # Build a small data frame with the score values
    metric_row = Row(
        seed = seed,
        rmse = pred_score["rmse"],
        mae = pred_score["mae"],
        r2 = pred_score["r2"],
        model_time = datetime.now(timezone.utc)  # Get the current time zone date time
    )

    # Compile a data frame
    metric_df = spark.createDataFrame([metric_row])

    # Insert these values into the PostgreSQL DB
    (
        metric_df.write
            .format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", "model_metric")
            .mode("append")
            .save()
    )


# This function will predict the next day's closing stock and crypto price
# It will use the model trained in the previous function
# Parameters include previously created data frame, choice between crypto and stock, 
# config data, model path, horizon days and window size
def predict_returns(df, entity, jdbc_url, model_path, horizon, days):
    # Load the trained model pipeline
    model = PipelineModel.load(model_path)

    # Pass the new data frame to run the model on and find the predicted close prices
    pred_df = model.transform(df)

    # Determine columns and target table based on entity type
    if entity == "stock":
        sym_col = "ticker"
        name_col = "company"
        cat_col = "sector"
        target_table = "processed_stock"
    else:
        sym_col = "symbol"
        name_col = "crypto"
        cat_col = "category"
        target_table = "processed_crypto"

    # Computing the window function for the volatility
    sym_col = "ticker" if entity == "stock" else "symbol"

    w = (
        Window.partitionBy(sym_col)
            .orderBy("market_date")
            .rowBetween(-days + 1, 0)
    )

    # Compute the predicted returns
    # Find the predicted volatility using the predicted returns
    # Add the amount of days we are looking ahead
    # Add the window size of the calculated volatility
    # Add the time zone sensitive time when this was run
    res_df = (
        pred_df
            .withColumn("predicted_return", (F.col("prediction") - F.col("close")) / F.col("close"))
            .withColumn("predicted_volatility", F.stddev("predicted_return").over(w))
            .withColumn("horizon", F.lit(horizon))
            .withColumn("vol_window", F.lit(days))
            .withColumn("prediction_time", F.current_timestamp())
    )

    # Select the column we want to write to
    output_df = res_df.select(
        sym_col,
        name_col,
        cat_col,
        "market_date",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "prediction",
        "predicted_return",
        "predicted_volatility",
        "horizon",
        "vol_window",
        "prediction_time"
    )

    # Write the predicted results into the database
    (
        output_df.write
            .format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", target_table)
            .mode("append")
            .save()
    )
