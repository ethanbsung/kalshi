-- rename_column
BEGIN;
ALTER TABLE spot_ticks RENAME COLUMN symbol TO product_id;
COMMIT;

-- recreate_table
BEGIN;
CREATE TABLE spot_ticks_new (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts INTEGER NOT NULL,
    product_id TEXT NOT NULL,
    price REAL,
    best_bid REAL,
    best_ask REAL,
    bid_qty REAL,
    ask_qty REAL,
    sequence_num INTEGER,
    raw_json TEXT
);

INSERT INTO spot_ticks_new (
    id,
    ts,
    product_id,
    price,
    best_bid,
    best_ask,
    bid_qty,
    ask_qty,
    sequence_num,
    raw_json
)
SELECT
    id,
    ts,
    symbol,
    price,
    best_bid,
    best_ask,
    bid_qty,
    ask_qty,
    sequence_num,
    raw_json
FROM spot_ticks;

DROP TABLE spot_ticks;
ALTER TABLE spot_ticks_new RENAME TO spot_ticks;
COMMIT;
