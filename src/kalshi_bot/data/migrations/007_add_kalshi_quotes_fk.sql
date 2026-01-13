BEGIN;
CREATE TABLE kalshi_quotes_new (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts INTEGER NOT NULL,
    market_id TEXT NOT NULL,
    yes_bid REAL,
    yes_ask REAL,
    no_bid REAL,
    no_ask REAL,
    yes_mid REAL,
    no_mid REAL,
    p_mid REAL,
    volume INTEGER,
    volume_24h INTEGER,
    open_interest INTEGER,
    raw_json TEXT,
    FOREIGN KEY (market_id) REFERENCES kalshi_markets(market_id)
);

INSERT INTO kalshi_quotes_new (
    id,
    ts,
    market_id,
    yes_bid,
    yes_ask,
    no_bid,
    no_ask,
    yes_mid,
    no_mid,
    p_mid,
    volume,
    volume_24h,
    open_interest,
    raw_json
)
SELECT
    id,
    ts,
    market_id,
    yes_bid,
    yes_ask,
    no_bid,
    no_ask,
    yes_mid,
    no_mid,
    p_mid,
    volume,
    volume_24h,
    open_interest,
    raw_json
FROM kalshi_quotes
WHERE market_id IN (SELECT market_id FROM kalshi_markets);

DROP TABLE kalshi_quotes;
ALTER TABLE kalshi_quotes_new RENAME TO kalshi_quotes;

CREATE INDEX IF NOT EXISTS idx_kalshi_quotes_market_ts
    ON kalshi_quotes(market_id, ts);
COMMIT;
