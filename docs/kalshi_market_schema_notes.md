# Kalshi Market Schema Notes (v2)

Sources:
- https://docs.kalshi.com/api-reference/market/get-markets
- https://docs.kalshi.com/api-reference/market/get-market

Fields relied on in `scripts/refresh_btc_markets.py`:
- `ticker`, `title`, `series_ticker`: used to identify and store BTC markets by series.
- `custom_strike`: used as the numeric strike when it is a number/string; dict/list values indicate multivariate markets and are stored with `strike=NULL`.
- `expiration_time`, `close_time`: used to populate `settlement_ts`.

Notes:
- The repo uses the fields above because they appear in `/markets` payloads in our environment.
- BTC discovery is intentionally constrained to `series_ticker` in `{"KXBTC", "KXBTC15M"}` and does not scan `/series`.
