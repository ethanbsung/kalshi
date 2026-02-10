PYTHONPATH=src .venv/bin/python scripts/refresh_btc_markets.py --status active --series KXBTC --series KXBTC15M --series KXBTCD
PYTHONPATH=src .venv/bin/python scripts/refresh_kalshi_contracts.py --status active --series KXBTC --series KXBTC15M --series KXBTCD
./scripts/live_stack.sh
rm -rf logs/refresh_btc_markets.log logs/refresh_kalshi_contracts.log logs/trader_tape.log
rm -f data/kalshi.sqlite data/kalshi.sqlite-wal data/kalshi.sqlite-shm
