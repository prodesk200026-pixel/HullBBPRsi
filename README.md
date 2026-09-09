# Bharati Universal Backend — Dhan L20 Auth Fixed

Universal market-data gateway for Bharati RSI / HMA / BBP PWA and future frontends.

## Critical authentication fix
Dhan allows access-token generation only once every 2 minutes. This build uses:
- one shared token-generation promise for concurrent startup callers
- token caching until the real expiry time
- a 2-minute cooldown after Dhan rate-limit responses
- no token generation inside the high-frequency option-chain loop
- health endpoints remain available even while Dhan authentication is unavailable

Recommended Render environment:
DHAN_CLIENT_ID
DHAN_PIN
DHAN_TOTP_SECRET

Alternatively set DHAN_ACCESS_TOKEN to a valid 24-hour Dhan token; when present, TOTP generation is disabled.

## Dhan data
- Live Market Feed FULL packet, RequestCode 21
- Dedicated Full Market Depth 20-level WebSocket, RequestCode 23
- Top bid/ask from Option Chain
- L20 bid/ask price, quantity and order count
- Option Chain
- Intraday history
- Analytics / model-derived proxy fields

L20 is parsed from Dhan's dedicated twentydepth feed, not reconstructed from the 5-level FULL packet.

## REST
GET /api/health
GET /api/status
GET /api/auth-status
GET /api/config
GET /api/state
GET /api/ticks
GET /api/tick?segment=...&securityId=...
GET /api/option-chain
GET /api/analytics
GET /api/depth?segment=NSE_FNO&securityId=...
GET /api/history?segment=IDX_I&securityId=13&interval=1
GET /api/instruments?search=NIFTY&limit=20

POST /api/index
POST /api/expiry
POST /api/expiry/select
POST /api/subscribe

## WebSocket
wss://YOUR-HOST/ws

Events:
hello
state
status
tick
optionChain
analytics
depth
error

No order placement is implemented. Dhan credentials stay server-side.
