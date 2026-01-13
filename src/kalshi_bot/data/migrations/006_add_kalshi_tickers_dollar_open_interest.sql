BEGIN;
ALTER TABLE kalshi_tickers ADD COLUMN dollar_open_interest INTEGER;
COMMIT;
