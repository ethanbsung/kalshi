BEGIN;
CREATE INDEX IF NOT EXISTS idx_kalshi_markets_status ON kalshi_markets(status);
CREATE INDEX IF NOT EXISTS idx_kalshi_quotes_market_ts ON kalshi_quotes(market_id, ts);
COMMIT;
