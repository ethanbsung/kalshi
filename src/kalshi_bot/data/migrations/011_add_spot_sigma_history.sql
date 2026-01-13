BEGIN;
CREATE TABLE IF NOT EXISTS spot_sigma_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts INTEGER NOT NULL,
    product_id TEXT NOT NULL,
    sigma REAL NOT NULL,
    source TEXT NOT NULL,
    reason TEXT
);

CREATE INDEX IF NOT EXISTS idx_spot_sigma_history_product_ts
    ON spot_sigma_history(product_id, ts);
COMMIT;
