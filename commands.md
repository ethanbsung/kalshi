PYTHONPATH=src .venv/bin/python -m kalshi_bot.app.refresh_btc_markets --status active --series KXBTC --series KXBTC15M --series KXBTCD
PYTHONPATH=src .venv/bin/python -m kalshi_bot.app.refresh_kalshi_contracts --status active --series KXBTC --series KXBTC15M --series KXBTCD
PYTHONPATH=src .venv/bin/python -m kalshi_bot.app.refresh_kalshi_settlements --status resolved --since-seconds 86400
./scripts/live_stack.sh
tail -f logs/trader_tape.log
PYTHONPATH=src .venv/bin/python scripts/report_open_shadow_positions.py
PYTHONPATH=src .venv/bin/python scripts/report_model_performance.py --since-seconds 86400
rm -rf logs/refresh_btc_markets.log logs/refresh_kalshi_contracts.log logs/trader_tape.log
rm -f data/kalshi.sqlite data/kalshi.sqlite-wal data/kalshi.sqlite-shm
