#!/usr/bin/env bash
set -euo pipefail

FAST="${FAST:-0}"
COINBASE_SECONDS=330
EDGE_ARGS=(
  --min-sigma-lookback-seconds 300
  --lookback-seconds 900
  --max-spot-points 2000
  --sigma-resample-seconds 5
)

python3 scripts/refresh_btc_markets.py
python3 scripts/refresh_kalshi_contracts.py --status active

if [[ "$FAST" == "1" ]]; then
  COINBASE_SECONDS=390
  python -m kalshi_bot.app.collector --coinbase --seconds "$COINBASE_SECONDS" --debug
  python3 scripts/poll_kalshi_quotes.py --seconds 90 --interval 5 --status active
  python3 scripts/compute_kalshi_edges.py \
    "${EDGE_ARGS[@]}" \
    --freshness-seconds 400 \
    --debug-market KXBTC --debug-market KXBTC15M --debug
else
  python -m kalshi_bot.app.collector --coinbase --seconds "$COINBASE_SECONDS" --debug
  python3 scripts/poll_kalshi_quotes.py --seconds 60 --interval 5 --status active
  python3 scripts/compute_kalshi_edges.py \
    "${EDGE_ARGS[@]}" \
    --debug-market KXBTC --debug-market KXBTC15M --debug
fi
