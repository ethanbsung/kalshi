BEGIN;
CREATE TABLE IF NOT EXISTS kalshi_edges (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts INTEGER NOT NULL,
    market_id TEXT NOT NULL,
    settlement_ts INTEGER,
    horizon_seconds INTEGER,
    spot_price REAL,
    sigma_annualized REAL,
    prob_yes REAL,
    yes_bid REAL,
    yes_ask REAL,
    no_bid REAL,
    no_ask REAL,
    ev_take_yes REAL,
    ev_take_no REAL,
    raw_json TEXT,
    FOREIGN KEY (market_id) REFERENCES kalshi_markets(market_id)
);

CREATE INDEX IF NOT EXISTS idx_kalshi_edges_market_ts
    ON kalshi_edges(market_id, ts);
CREATE INDEX IF NOT EXISTS idx_kalshi_edges_ts
    ON kalshi_edges(ts);
CREATE INDEX IF NOT EXISTS idx_kalshi_edges_settlement_ts
    ON kalshi_edges(settlement_ts);
COMMIT;
