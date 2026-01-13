BEGIN;
ALTER TABLE kalshi_markets ADD COLUMN close_ts INTEGER;
ALTER TABLE kalshi_markets ADD COLUMN expected_expiration_ts INTEGER;
ALTER TABLE kalshi_contracts ADD COLUMN close_ts INTEGER;
ALTER TABLE kalshi_contracts ADD COLUMN expected_expiration_ts INTEGER;
COMMIT;
