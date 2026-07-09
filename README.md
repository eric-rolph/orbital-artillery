# 🛰️ Orbital Artillery

A **local-multiplayer, browser-based 2D artillery game**. One shared display (a TV
or monitor) is the **Screen**; each player's **smartphone** is the gamepad. Curve
your shots through the gravity of two planets and blast the other player's turret.
First to **3 hits** wins.

Built to demonstrate a **WebRTC multi-peer architecture** with a **Cloudflare
Worker + SQLite Durable Object** doing nothing but the handshake — after that,
every input flows **peer-to-peer**, so the server carries zero gameplay bandwidth
and latency is bounded only by the local network.

```
 ┌─────────────┐   WebSocket    ┌──────────────────────┐   WebSocket   ┌──────────────┐
 │  Screen     │ ─────────────► │  Durable Object      │ ◄──────────── │ Controller 1 │
 │ (desktop)   │   (signaling)  │  (room, code, relay) │  (signaling)  │  (phone)     │
 │             │ ◄───────────── │                      │ ────────────► │              │
 └──────┬──────┘                └──────────────────────┘               └──────┬───────┘
        │                                                                     │
        │           RTCDataChannel  (P2P inputs — server is OUT of the path)  │
        └─────────────────────────────────────────────────────────────────────┘
                 (a second independent peer + channel exists for Controller 2)
```

## How to play

1. Open the deployed URL on the shared screen → **"Open the Screen"**. A 4-letter
   room code (and QR code) appears.
2. On each phone, open the same site → **"Be a Controller"**, or just scan the QR.
   Enter the code. You're assigned Player 1 or Player 2.
3. When both phones connect, the match starts. **Aim** with the dial, set **power**
   with the slider, tap **FIRE**.

## Files

| File | Role |
| --- | --- |
| `worker.js` | Worker router + `RoomDurableObject` (room creation, player-slot assignment, WebSocket signaling relay). |
| `public/screen.html` / `screen.js` | Desktop display. Runs **two independent** `RTCPeerConnection`s (one per phone), owns the authoritative game state, and runs the fixed-step gravitational physics + canvas renderer. |
| `public/controller.html` / `controller.js` | Mobile gamepad. WebRTC *answerer*; streams aim/power/fire over the data channel. Viewport locked against zoom/scroll. |
| `public/index.html` | Landing page routing desktops → Screen, phones → Controller. |
| `wrangler.toml` | Worker + Static Assets + Durable Object (SQLite) config. |

## The multi-peer trick

WebRTC is peer-to-peer with no built-in one-to-many. The Screen therefore keeps a
`Map<playerId, Peer>` — a **separate** `RTCPeerConnection`, `RTCDataChannel`, and
ICE queue per phone — all multiplexed over one signaling WebSocket. Every
signaling message is tagged with a player id (`target` outbound, `from` inbound)
so the Durable Object routes each phone's SDP/ICE to the right connection. The
Screen is always the offerer and each phone the answerer, so roles are fixed and
there's no glare to reconcile. See the long comment blocks in `worker.js` and
`screen.js`.

## Develop

```bash
npm install
npm run dev        # wrangler dev — open the printed URL as the Screen,
                   # and http://<lan-ip>:8787/controller.html on your phone
```

## Deploy

Pushing to `main` triggers `.github/workflows/deploy.yml`, which runs
`wrangler deploy`. Configure two repo secrets:

- `CLOUDFLARE_API_TOKEN` — token with **Workers Scripts: Edit** (and Account →
  Workers KV/DO as needed).
- `CLOUDFLARE_ACCOUNT_ID` — your account id.

Manual deploy: `npm run deploy`.

## Tech notes

- **Determinism**: physics runs on a fixed `1/120s` timestep in a logical
  `1600×900` world, letterboxed to any monitor, so behavior is resolution-independent.
- **Gravity**: each projectile is integrated under the summed inverse-square pull
  of both planets (with a softening term). A live trajectory preview ghosts the
  same integrator.
- **Reliability**: the input data channel is ordered+reliable — on a LAN,
  retransmits are negligible and a dropped FIRE would be unacceptable.
- **No TURN**: only STUN is configured. Relaying would drag a server back into the
  data path; on the same Wi-Fi, host candidates connect directly.
- **Relay fallback**: STUN alone can't punch every network (phone on cellular,
  Wi-Fi AP/client isolation, symmetric NAT). If a controller's data channel
  hasn't opened ~4 s after joining — or the connection fails outright — it
  transparently falls back to sending inputs over its signaling WebSocket and
  the Durable Object forwards them to the screen (`RELAY` shows next to the
  room code). P2P remains the primary path and wins back automatically if it
  ever completes. Force it with `controller.html?code=XXXX&relay=1`.
- **E2E harness**: open `/harness.html` (or `/harness.html?relay=1`) to run the
  real screen + controller side by side in one tab and drive a scripted
  join → aim → power → fire pass across the full stack.
