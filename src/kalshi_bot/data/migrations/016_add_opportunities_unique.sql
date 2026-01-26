BEGIN;
CREATE UNIQUE INDEX IF NOT EXISTS idx_opportunities_unique
    ON opportunities(ts_eval, market_id, side);
COMMIT;
