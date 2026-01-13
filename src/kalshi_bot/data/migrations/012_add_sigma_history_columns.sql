BEGIN;
ALTER TABLE spot_sigma_history ADD COLUMN method TEXT;
ALTER TABLE spot_sigma_history ADD COLUMN lookback_seconds INTEGER;
ALTER TABLE spot_sigma_history ADD COLUMN points INTEGER;
COMMIT;
