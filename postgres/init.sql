-- Storing company data from the CSV file
CREATE TABLE IF NOT EXISTS company_data (
    symbol TEXT PRIMARY KEY,
    name TEXT,
    sector TEXT,
    industry TEXT
);

-- Storing current stock records
CREATE TABLE IF NOT EXISTS stock_bars (
    id SERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    open DOUBLE PRECISION NOT NULL,
    high DOUBLE PRECISION NOT NULL,
    low DOUBLE PRECISION NOT NULL,
    close DOUBLE PRECISION NOT NULL,
    volume BIGINT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_stock_bars
    ON stock_bars(symbol);

-- Storing computed metric data
CREATE TABLE IF NOT EXISTS stock_metrics (
    id SERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    vwap DOUBLE PRECISION NOT NULL,
    vol_5m DOUBLE PRECISION NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_stock_metrics
    ON stock_metrics(symbol);