#!/usr/bin/env bash
set -euo pipefail

python3 scripts/refresh_btc_markets.py
python3 scripts/refresh_kalshi_contracts.py --status active
python3 scripts/poll_kalshi_quotes.py --seconds 60 --interval 5 --status active
python -m kalshi_bot.app.collector --coinbase --seconds 30 --debug
python3 scripts/compute_kalshi_edges.py --debug-market KXBTC --debug-market KXBTC15M --debug
