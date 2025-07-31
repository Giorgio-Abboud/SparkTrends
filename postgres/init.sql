CREATE TABLE IF NOT EXISTS company_data (
    symbol TEXT PRIMARY KEY,
    name TEXT,
    sector TEXT,
    industry TEXT
);
-- index for reference not needed here
CREATE INDEX IF NOT EXISTS idx_company_data 
    ON company_data(ticker);
