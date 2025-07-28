-- RAW DATA TABLES FROM THE KAFKA CONSUMER BEFORE SPARK PROCESSING
-- INDEXING FOR NEWS AND COMPOSITE INDEXING FOR STOCK AND CRYPTO FASTER LOOKUPS
CREATE TABLE IF NOT EXISTS raw_news (
    id SERIAL PRIMARY KEY NOT NULL,
    ticker TEXT,
    company TEXT,
    sector TEXT,
    time_published TIMESTAMPTZ,
    news_info JSONB,
    ingestion_time TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_raw_news_ticker_time 
    ON raw_news(ticker, time_published);

CREATE TABLE IF NOT EXISTS raw_stock (
    id SERIAL PRIMARY KEY NOT NULL,
    ticker TEXT,
    company TEXT,
    sector TEXT,
    market_date DATE,
    stock_info JSONB,
    ingestion_time TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_raw_stock_ticker_date 
    ON raw_stock(ticker, market_date);

CREATE TABLE IF NOT EXISTS raw_crypto (
    id SERIAL PRIMARY KEY NOT NULL,
    symbol TEXT,
    crypto TEXT,
    category TEXT,
    market_date DATE,
    crypto_info JSONB,
    ingestion_time TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_raw_crypto_symbol_date 
    ON raw_crypto(symbol, market_date);


-- PROCESSED DATA TABLES TO STORE VALUES AFTER SPARK PROCESSING
-- STOCK AND CRYPTO SENTIMENT WILL BE ADDED THROUGH A JOIN CLAUSE
CREATE TABLE IF NOT EXISTS processed_news (
    id SERIAL PRIMARY KEY NOT NULL,
    ticker TEXT NOT NULL,
    company TEXT,
    sector TEXT,
    time_published TIMESTAMPTZ,
    title TEXT,
    summary TEXT,
    news_url TEXT,
    key_phrases TEXT[],
    news_sentiment DOUBLE,
    ingestion_time TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_processed_news_ticker_time 
    ON processed_news(ticker, time_published);
    
CREATE TABLE IF NOT EXISTS processed_stock (
    -- Standard Stock Data
    id SERIAL PRIMARY KEY NOT NULL,
    ticker TEXT NOT NULL,
    company TEXT,
    sector TEXT,
    market_date DATE NOT NULL,
    stock_returns DOUBLE,
    stock_volatility DOUBLE,
    vol_window INTEGER,  -- Volatility window of how many days
    stock_sentiment DOUBLE,
    ingestion_time TIMESTAMPTZ DEFAULT now(),

    -- Stock Predictions
    predicted_return DOUBLE,      -- Future stock returns
    predicted_volatility DOUBLE,  -- Future volatility
    horizon INTEGER,              -- How many days ahead to predict
    prediction_time TIMESTAMPTZ   -- When the model ran
);
CREATE INDEX IF NOT EXISTS idx_processed_stock_ticker_date 
    ON processed_stock(ticker, market_date);

CREATE TABLE IF NOT EXISTS processed_crypto (
    -- Standard Crypto Data
    id SERIAL PRIMARY KEY NOT NULL,
    symbol TEXT NOT NULL,
    crypto TEXT,
    category TEXT,
    market_date DATE NOT NULL,
    crypto_returns DECIMAL,
    crypto_volatility DECIMAL,
    vol_window INTEGER,  -- Volatility window of how many days
    crypto_sentiment DECIMAL,
    ingestion_time TIMESTAMPTZ DEFAULT now(),

    -- Stock Predictions
    predicted_return DOUBLE,      -- Future crypto returns
    predicted_volatility DOUBLE,  -- Future volatility
    horizon INTEGER,              -- How many days ahead to predict
    prediction_time TIMESTAMPTZ   -- When the model ran
);
CREATE INDEX IF NOT EXISTS idx_processed_crypto_symbol_date 
    ON processed_crypto(symbol, market_date);


-- MACHINE LEARNING MODEL METRIC TABLE WITH THE ACCURACY, SEED, AND TIME WHEN RUN
CREATE TABLE IF NOT EXISTS model_metric (
    id SERIAL PRIMARY KEY NOT NULL,
    seed INTEGER NOT NULL,
    rmse DOUBLE,  -- RMSE confidence
    mae DOUBLE,   -- MAE confidence
    r2 DOUBLE,    -- R^2 confidence
    model_time TIMESTAMPTZ
)
CREATE INDEX IF NOT EXISTS idx_model_metric 
    ON model_metric(seed);