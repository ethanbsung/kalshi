import time
import requests

BASE = "https://api.elections.kalshi.com/trade-api/v2"

def fetch_page(cursor=None, limit=1000, status="open"):
    params = {"limit": limit, "status": status}
    if cursor:
        params["cursor"] = cursor
    r = requests.get(f"{BASE}/markets", params=params, timeout=20)
    r.raise_for_status()
    return r.json()

def score(m):
    vol24 = m.get("volume_24h") or 0
    liq = m.get("liquidity") or 0
    oi = m.get("open_interest") or 0
    # prefer volume if it's actually nonzero; otherwise liquidity/OI are better “activity” proxies
    return (vol24, liq, oi)

def main(max_pages=20):
    cursor = None
    all_mkts = []
    for page in range(1, max_pages + 1):
        d = fetch_page(cursor=cursor, limit=1000, status="open")
        mkts = d.get("markets", [])
        all_mkts.extend(mkts)
        cursor = d.get("cursor")
        print(f"Fetched page {page}, markets={len(mkts)}, total={len(all_mkts)}")
        if not cursor:
            break
        time.sleep(0.25)

    all_mkts.sort(key=score, reverse=True)
    print("\nTOP 20 (vol24h, liquidity, open_interest):\n")
    for m in all_mkts[:20]:
        print(
            m.get("ticker"),
            "vol24h=", m.get("volume_24h"),
            "liq=", m.get("liquidity"),
            "oi=", m.get("open_interest"),
            "yes_bid=", m.get("yes_bid"),
            "yes_ask=", m.get("yes_ask"),
            "no_bid=", m.get("no_bid"),
            "no_ask=", m.get("no_ask"),
        )

    # print a single best ticker for copy/paste
    if all_mkts:
        print("\nBEST_TICKER:", all_mkts[0].get("ticker"))

if __name__ == "__main__":
    main()