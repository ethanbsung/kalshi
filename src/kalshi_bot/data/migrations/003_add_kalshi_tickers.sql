BEGIN;
CREATE TABLE IF NOT EXISTS kalshi_tickers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts INTEGER NOT NULL,
    market_id TEXT NOT NULL,
    price REAL,
    best_yes_bid REAL,
    best_yes_ask REAL,
    best_no_bid REAL,
    best_no_ask REAL,
    volume INTEGER,
    open_interest INTEGER,
    dollar_volume INTEGER,
    dollar_oapen_interest INTEGER,
    raw_json TEXT
);
COMMIT;
