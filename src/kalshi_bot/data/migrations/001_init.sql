BEGIN;
CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER PRIMARY KEY,
    applied_ts INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS spot_ticks (
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

CREATE TABLE IF NOT EXISTS kalshi_markets (
    market_id TEXT PRIMARY KEY,
    ts_loaded INTEGER NOT NULL,
    title TEXT,
    strike REAL,
    settlement_ts INTEGER,
    status TEXT,
    raw_json TEXT
);

CREATE TABLE IF NOT EXISTS kalshi_orderbook_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts INTEGER NOT NULL,
    market_id TEXT NOT NULL,
    seq INTEGER,
    yes_bids_json TEXT,
    no_bids_json TEXT,
    raw_json TEXT
);

CREATE TABLE IF NOT EXISTS kalshi_orderbook_deltas (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts INTEGER NOT NULL,
    market_id TEXT NOT NULL,
    seq INTEGER,
    side TEXT NOT NULL,
    price REAL NOT NULL,
    size REAL NOT NULL,
    raw_json TEXT
);

CREATE TABLE IF NOT EXISTS opportunities (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts_eval INTEGER NOT NULL,
    market_id TEXT NOT NULL,
    settlement_ts INTEGER,
    strike REAL,
    spot_price REAL,
    sigma REAL,
    tau REAL,
    p_model REAL,
    p_market REAL,
    best_yes_bid REAL,
    best_yes_ask REAL,
    best_no_bid REAL,
    best_no_ask REAL,
    spread REAL,
    eligible INTEGER NOT NULL,
    reason_not_eligible TEXT,
    would_trade INTEGER NOT NULL,
    side TEXT,
    ev_raw REAL,
    ev_net REAL,
    cost_buffer REAL,
    raw_json TEXT
);

CREATE TABLE IF NOT EXISTS orders (
    order_id TEXT PRIMARY KEY,
    client_order_id TEXT,
    market_id TEXT NOT NULL,
    ts_sent INTEGER NOT NULL,
    side TEXT NOT NULL,
    limit_price REAL NOT NULL,
    qty INTEGER NOT NULL,
    linked_opportunity_id INTEGER,
    cancelled INTEGER NOT NULL,
    cancel_reason TEXT,
    raw_json TEXT
);

CREATE TABLE IF NOT EXISTS fills (
    fill_id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL,
    market_id TEXT NOT NULL,
    ts_fill INTEGER NOT NULL,
    side TEXT NOT NULL,
    price REAL NOT NULL,
    qty INTEGER NOT NULL,
    fee REAL,
    raw_json TEXT
);

CREATE TABLE IF NOT EXISTS positions_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts INTEGER NOT NULL,
    market_id TEXT NOT NULL,
    position_qty INTEGER NOT NULL,
    avg_price REAL,
    pnl REAL,
    raw_json TEXT
);

CREATE TABLE IF NOT EXISTS settlements (
    market_id TEXT PRIMARY KEY,
    outcome INTEGER NOT NULL,
    settlement_price REAL,
    ts_settlement INTEGER NOT NULL,
    raw_json TEXT
);

CREATE TABLE IF NOT EXISTS features (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts INTEGER NOT NULL,
    market_id TEXT NOT NULL,
    name TEXT NOT NULL,
    value REAL,
    raw_json TEXT
);
COMMIT;
