# Kalshi API Contract (Authoritative)

When working with Kalshi APIs, **do not guess** fields, behavior, or message formats.

You MUST reference the official docs below and only implement behavior explicitly
described there.

## REST API
Base URL:
https://api.elections.kalshi.com/trade-api/v2

Docs:
https://docs.kalshi.com/

Key endpoints used:
- GET /markets
- GET /portfolio/positions
- POST /orders
- DELETE /orders/{id}

## WebSocket API (v2)
Base URL:
wss://api.elections.kalshi.com/trade-api/ws/v2

Docs:
- Connection:
  https://docs.kalshi.com/websockets/websocket-connection
- Keep-alive:
  https://docs.kalshi.com/websockets/connection-keep-alive
- Orderbook updates:
  https://docs.kalshi.com/websockets/orderbook-updates
- Market ticker:
  https://docs.kalshi.com/websockets/market-ticker
- Quick start:
  https://docs.kalshi.com/getting_started/quick_start_websockets

## Non-negotiable rules
- Do NOT invent fields or message types
- Do NOT assume undocumented guarantees
- Do NOT change parsing logic without checking docs
- If docs are ambiguous, log and fail safe
- If unsure, add TODO and do NOT implement speculative behavior