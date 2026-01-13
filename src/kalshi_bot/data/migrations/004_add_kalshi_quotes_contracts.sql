BEGIN;
CREATE TABLE IF NOT EXISTS kalshi_quotes (
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
    raw_json TEXT
);

CREATE TABLE IF NOT EXISTS kalshi_contracts (
    ticker TEXT PRIMARY KEY,
    lower REAL,
    upper REAL,
    strike_type TEXT,
    settlement_ts INTEGER,
    updated_ts INTEGER NOT NULL
);
COMMIT;
