BEGIN;
ALTER TABLE kalshi_markets ADD COLUMN expiration_ts INTEGER;
ALTER TABLE kalshi_contracts ADD COLUMN expiration_ts INTEGER;
COMMIT;
