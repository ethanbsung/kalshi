BEGIN;

DELETE FROM kalshi_edge_snapshots
WHERE id IN (
    SELECT s.id
    FROM kalshi_edge_snapshots s
    JOIN (
        SELECT market_id, asof_ts, MIN(id) AS keep_id
        FROM kalshi_edge_snapshots
        GROUP BY market_id, asof_ts
        HAVING COUNT(*) > 1
    ) d
      ON s.market_id = d.market_id
     AND s.asof_ts = d.asof_ts
    WHERE s.id <> d.keep_id
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_kalshi_edge_snapshots_unique_market_asof
    ON kalshi_edge_snapshots(market_id, asof_ts);

COMMIT;
