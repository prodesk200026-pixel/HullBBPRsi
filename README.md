# Bharati Universal Backend — Dhan

Production-oriented Dhan market-data gateway for the Bharati RSI PWA.

REST:
GET /api/health
GET /api/status
GET /api/config
GET /api/state
GET /api/ticks
GET /api/option-chain
GET /api/analytics
GET /api/depth?segment=NSE_FNO&securityId=...
GET /api/history?segment=IDX_I&securityId=13&interval=1
GET /api/instruments?search=NIFTY&limit=20

POST /api/index
POST /api/expiry
POST /api/expiry/select
POST /api/subscribe

WebSocket:
wss://HOST/ws

Dhan:
- Live Feed FULL packet (RequestCode 21)
- Full Market Depth 20-level (RequestCode 23)
- Option Chain API
- Intraday historical candles
- TOTP authentication or 24-hour access token
- Dhan credentials remain server-side

L20 is parsed from Dhan's dedicated twentydepth WebSocket. It is not reconstructed from the 5-level packet.

No order placement is implemented in this backend.
