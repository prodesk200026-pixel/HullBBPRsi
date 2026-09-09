# Bharati Universal Backend — Dhan L20 — FIXED

## Render crash fixed
The previous deployment failed with:
`TypeError: WebSocket.Server is not a constructor`

Root cause: the `ws` package is used through ES modules. `WebSocket.Server` is not the correct constructor in this import form.

### Correct technical fix
```js
import WebSocket, { WebSocketServer } from "ws";
const gateway = new WebSocketServer({ server, path: "/ws" });
```

This ZIP has that fix already applied.

## Render settings
- Root Directory: blank (files are in repository root)
- Build Command: `npm install`
- Start Command: `npm start`
- Health Check Path: `/api/health`

## Backend role
Dhan market-data gateway only. Frontends connect through REST + `/ws`; Dhan credentials remain server-side.
