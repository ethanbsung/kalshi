BEGIN;
ALTER TABLE kalshi_contracts ADD COLUMN settled_ts INTEGER;
ALTER TABLE kalshi_contracts ADD COLUMN outcome INTEGER;

CREATE TABLE IF NOT EXISTS kalshi_edge_snapshot_scores (
    asof_ts INTEGER NOT NULL,
    market_id TEXT NOT NULL,
    settled_ts INTEGER,
    outcome INTEGER,
    pnl_take_yes REAL,
    pnl_take_no REAL,
    brier REAL,
    logloss REAL,
    error TEXT,
    created_ts INTEGER NOT NULL,
    PRIMARY KEY (market_id, asof_ts),
    FOREIGN KEY (market_id, asof_ts)
        REFERENCES kalshi_edge_snapshots(market_id, asof_ts)
);

CREATE INDEX IF NOT EXISTS idx_kalshi_edge_snapshot_scores_settled_ts
    ON kalshi_edge_snapshot_scores(settled_ts);
CREATE INDEX IF NOT EXISTS idx_kalshi_edge_snapshot_scores_created_ts
    ON kalshi_edge_snapshot_scores(created_ts);
COMMIT;
