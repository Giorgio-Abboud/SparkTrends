-- RAW DATA TABLES FROM THE KAFKA CONSUMER BEFORE SPARK PROCESSING
-- INDEXING FOR NEWS AND COMPOSITE INDEXING FOR STOCK AND CRYPTO FASTER LOOKUPS
CREATE TABLE IF NOT EXISTS raw_news (
    id SERIAL PRIMARY KEY NOT NULL,
    ticker TEXT,
    company TEXT,
    sector TEXT,
    time_published TIMESTAMPTZ,
    news_info JSONB,
    date_received TIMESTAMPTZ DEFAULT now()
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
    date_received TIMESTAMPTZ DEFAULT now()
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
    date_received TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_raw_crypto_symbol_date 
    ON raw_crypto(symbol, market_date);
