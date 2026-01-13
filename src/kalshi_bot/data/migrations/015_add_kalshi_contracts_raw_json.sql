BEGIN;
ALTER TABLE kalshi_contracts ADD COLUMN raw_json TEXT;
COMMIT;
