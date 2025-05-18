CREATE TABLE IF NOT EXISTS news (
    id INT PRIMARY KEY NOT NULL,
    source_id TEXT,
    headline TEXT,
    published_at TIMESTAMPTZ,
    source_from TEXT,
    site_url TEXT
);

CREATE TABLE IF NOT EXISTS stocks (
    id INT PRIMARY KEY NOT NULL,
    ticker TEXT,
    price FLOAT,
    published_at TIMESTAMPTZ
);
