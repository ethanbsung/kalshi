BEGIN;
CREATE TABLE IF NOT EXISTS kalshi_edge_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    asof_ts INTEGER NOT NULL,
    market_id TEXT NOT NULL,
    settlement_ts INTEGER,
    spot_ts INTEGER NOT NULL,
    spot_price REAL NOT NULL,
    sigma_annualized REAL NOT NULL,
    prob_yes REAL NOT NULL,
    prob_yes_raw REAL,
    horizon_seconds INTEGER,
    quote_ts INTEGER,
    yes_bid REAL,
    yes_ask REAL,
    no_bid REAL,
    no_ask REAL,
    yes_mid REAL,
    no_mid REAL,
    ev_take_yes REAL,
    ev_take_no REAL,
    spot_age_seconds INTEGER,
    quote_age_seconds INTEGER,
    skip_reason TEXT,
    raw_json TEXT NOT NULL,
    FOREIGN KEY (market_id) REFERENCES kalshi_markets(market_id)
);

CREATE INDEX IF NOT EXISTS idx_kalshi_edge_snapshots_market_asof
    ON kalshi_edge_snapshots(market_id, asof_ts);
CREATE UNIQUE INDEX IF NOT EXISTS idx_kalshi_edge_snapshots_unique_market_asof
    ON kalshi_edge_snapshots(market_id, asof_ts);
CREATE INDEX IF NOT EXISTS idx_kalshi_edge_snapshots_asof
    ON kalshi_edge_snapshots(asof_ts);
CREATE INDEX IF NOT EXISTS idx_kalshi_edge_snapshots_settlement_ts
    ON kalshi_edge_snapshots(settlement_ts);
COMMIT;
