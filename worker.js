/**
 * Orbital Artillery — Signaling Server
 * =====================================
 *
 * A Cloudflare Worker (ES Modules) fronting a single SQLite-backed Durable Object
 * per game room. Its ONLY job is to broker the WebRTC handshake between:
 *
 *      [ Desktop "Screen" ]  <-- WebSocket -->  [ Durable Object ]  <-- WebSocket -->  [ Phone "Controller" ]
 *
 * ...so the two ends can discover each other and exchange SDP + ICE. The instant
 * the peer-to-peer RTCDataChannel opens, all gameplay traffic flows DIRECTLY
 * phone -> screen over WebRTC and the Worker/DO carry ZERO further bytes. This is
 * the whole point: the server pays for a few hundred bytes of handshake, then
 * steps out of the hot path so input latency is bounded only by the local network.
 *
 * Topology
 * --------
 * One room == one Durable Object instance, addressed by a 4-letter code via
 * `idFromName(code)`. The Screen opens the room (and is handed the code); up to
 * two Controllers join with that code. The DO tags every socket with a role so it
 * can route a Controller's signaling to the (single) Screen, and route the
 * Screen's signaling to a *specific* Controller by player id. That per-player
 * routing is what lets one Screen run two INDEPENDENT peer connections at once
 * (see screen.js for the client-side multi-peer mesh).
 *
 * We use the WebSocket Hibernation API (`state.acceptWebSocket`) so the DO can be
 * evicted from memory between messages without dropping connections — the runtime
 * rehydrates it on the next frame. Because of that, we never hold socket refs in
 * instance fields; we look them up by tag (`getWebSockets(tag)`) and stash
 * per-socket metadata with `serializeAttachment()`.
 */

// ---------------------------------------------------------------------------
// Worker entrypoint — pure router. Static files (screen/controller HTML+JS) are
// served by Cloudflare's Static Assets pipeline BEFORE this runs; we only see
// requests that don't match a file on disk, i.e. our two WebSocket endpoints.
// ---------------------------------------------------------------------------
export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    // NOTE: these routes are deliberately under /api/* so they can never collide
    // with a static asset filename. Cloudflare's Static Assets layer runs BEFORE
    // the Worker and, with default html_handling, would map a bare "/screen" to
    // the file "/screen.html" — the request would never reach this handler.
    switch (url.pathname) {
      case '/api/screen':
        // A desktop wants to host a new room.
        return openRoomAsScreen(request, env);
      case '/api/join':
        // A phone wants to join an existing room by code.
        return joinRoomAsController(request, env);
      case '/health':
        return new Response('ok', { headers: { 'content-type': 'text/plain' } });
      default:
        // Anything else: hand back to the static asset server. In normal
        // operation Static Assets already handled these; this is a safety net
        // (and covers `run_worker_first` setups).
        if (env.ASSETS) return env.ASSETS.fetch(request);
        return new Response('Not found', { status: 404 });
    }
  },
};

// Unambiguous alphabet: no I/O/0/1 so a code read off a TV can't be mistyped.
const CODE_ALPHABET = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';

/** Cryptographically-random 4-char room code. 32^4 ≈ 1M combinations. */
function makeRoomCode() {
  const bytes = new Uint32Array(4);
  crypto.getRandomValues(bytes);
  let code = '';
  for (let i = 0; i < 4; i++) code += CODE_ALPHABET[bytes[i] % CODE_ALPHABET.length];
  return code;
}

/**
 * Desktop screen connects here with no code. We mint a fresh code, resolve the
 * matching Durable Object, and forward the WebSocket upgrade to it. The DO echoes
 * the code back to the screen over the socket so it can be shown on-screen.
 */
function openRoomAsScreen(request, env) {
  if (request.headers.get('Upgrade') !== 'websocket') {
    return new Response('Expected a WebSocket upgrade', { status: 426 });
  }
  const code = makeRoomCode();
  return forwardToRoom(request, env, code, 'screen');
}

/**
 * Phone connects here with `?code=ABCD`. We validate the shape, resolve the same
 * DO the screen created, and forward the upgrade tagged as a controller. The DO
 * decides whether the room exists / has space and assigns a player slot.
 */
function joinRoomAsController(request, env) {
  if (request.headers.get('Upgrade') !== 'websocket') {
    return new Response('Expected a WebSocket upgrade', { status: 426 });
  }
  const url = new URL(request.url);
  const code = (url.searchParams.get('code') || '').toUpperCase();
  if (!/^[A-Z2-9]{4}$/.test(code)) {
    return new Response('Invalid room code', { status: 400 });
  }
  return forwardToRoom(request, env, code, 'controller');
}

/** Resolve the DO for `code` and forward the upgrade, annotating role + code. */
function forwardToRoom(request, env, code, role) {
  const id = env.ROOMS.idFromName(code);
  const stub = env.ROOMS.get(id);
  const doUrl = new URL(request.url);
  doUrl.searchParams.set('code', code);
  doUrl.searchParams.set('role', role);
  // Re-wrap the request so the Upgrade header + method survive the internal hop.
  return stub.fetch(new Request(doUrl.toString(), request));
}

// ===========================================================================
// Durable Object — one live game room.
// ===========================================================================
export class RoomDurableObject {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.sql = state.storage.sql; // SQLite storage backend (see wrangler migration).

    // Schema init must complete before any request touches storage. Wrapping it
    // in blockConcurrencyWhile also guards against the DO being rehydrated from
    // hibernation mid-flight.
    state.blockConcurrencyWhile(async () => {
      this.sql.exec(`
        CREATE TABLE IF NOT EXISTS room (
          id         INTEGER PRIMARY KEY CHECK (id = 1),
          code       TEXT,
          created_at INTEGER
        );
      `);
      this.sql.exec(`
        CREATE TABLE IF NOT EXISTS players (
          slot       INTEGER PRIMARY KEY,   -- player id: 1 or 2
          joined_at  INTEGER,
          connected  INTEGER NOT NULL DEFAULT 1
        );
      `);
    });
  }

  // --- HTTP entry: every request here is a WebSocket upgrade forwarded by the Worker.
  async fetch(request) {
    const url = new URL(request.url);
    const role = url.searchParams.get('role');
    const code = (url.searchParams.get('code') || '').toUpperCase();

    const { 0: client, 1: server } = new WebSocketPair();

    if (role === 'screen') {
      this.attachScreen(server, code);
    } else if (role === 'controller') {
      this.attachController(server);
    } else {
      return new Response('Unknown role', { status: 400 });
    }

    return new Response(null, { status: 101, webSocket: client });
  }

  /** Register the desktop screen socket and hand it its room code. */
  attachScreen(server, code) {
    // Persist the room record (durable across hibernation / eviction).
    this.sql.exec(
      `INSERT INTO room (id, code, created_at) VALUES (1, ?, ?)
         ON CONFLICT(id) DO UPDATE SET code = excluded.code, created_at = excluded.created_at`,
      code,
      Date.now(),
    );

    // Accept with hibernation. Tag 'screen' so getWebSockets('screen') finds it.
    this.state.acceptWebSocket(server, ['screen']);
    server.serializeAttachment({ role: 'screen' });

    server.send(JSON.stringify({ type: 'room-created', code }));

    // Reconnect case: if controllers are already live, replay their presence so
    // a refreshed screen can rebuild its peer connections.
    for (const c of this.state.getWebSockets('controller')) {
      const meta = c.deserializeAttachment();
      if (meta?.slot) server.send(JSON.stringify({ type: 'player-joined', playerId: meta.slot }));
    }
  }

  /** Register a joining phone: validate room, assign a slot, notify both ends. */
  attachController(server) {
    // IMPORTANT: acceptWebSocket() may be called AT MOST ONCE per socket. So we
    // decide the outcome BEFORE accepting (getWebSockets() lookups are allowed
    // pre-accept), then accept exactly once on whichever branch we take.
    const screens = this.state.getWebSockets('screen');
    if (screens.length === 0) {
      this.state.acceptWebSocket(server, ['reject']);
      server.send(JSON.stringify({ type: 'error', reason: 'no-room', message: 'No screen is hosting this room.' }));
      server.close(4004, 'no-room');
      return;
    }

    // Determine the lowest free slot from the LIVE controller sockets (the real
    // source of truth). SQLite mirrors it for durability/analytics.
    const used = new Set(
      this.state.getWebSockets('controller').map((ws) => ws.deserializeAttachment()?.slot).filter(Boolean),
    );
    let slot = null;
    for (const s of [1, 2]) {
      if (!used.has(s)) { slot = s; break; }
    }
    if (slot === null) {
      this.state.acceptWebSocket(server, ['reject']);
      server.send(JSON.stringify({ type: 'error', reason: 'full', message: 'This room already has two players.' }));
      server.close(4001, 'full');
      return;
    }

    // Tag with role AND player slot so the screen can target this exact phone.
    this.state.acceptWebSocket(server, ['controller', `p${slot}`]);
    server.serializeAttachment({ role: 'controller', slot });

    this.sql.exec(
      `INSERT INTO players (slot, joined_at, connected) VALUES (?, ?, 1)
         ON CONFLICT(slot) DO UPDATE SET joined_at = excluded.joined_at, connected = 1`,
      slot,
      Date.now(),
    );

    // Tell the phone who it is; tell the screen a peer appeared (screen then offers).
    server.send(JSON.stringify({ type: 'joined', playerId: slot }));
    for (const s of screens) s.send(JSON.stringify({ type: 'player-joined', playerId: slot }));
  }

  // -------------------------------------------------------------------------
  // Hibernation message handler — the ONLY signaling relay in the system.
  //
  // Controller -> Screen:  {type:'signal', data}          becomes  {type:'signal', from: <slot>, data}
  // Screen -> Controller:  {type:'signal', target, data}  becomes  {type:'signal', from:'screen', data}
  //
  // `data` is an opaque WebRTC payload (an SDP description or an ICE candidate);
  // the DO never inspects it. Once the DataChannel is up, no more of these arrive.
  // -------------------------------------------------------------------------
  webSocketMessage(ws, message) {
    const meta = ws.deserializeAttachment() || {};
    let msg;
    try {
      msg = JSON.parse(message);
    } catch {
      return; // ignore malformed frames
    }
    if (msg.type !== 'signal') return;

    if (meta.role === 'controller') {
      // Fan a controller's signal to the screen, stamped with the sender's slot
      // so the screen knows which of its two peer connections it belongs to.
      for (const s of this.state.getWebSockets('screen')) {
        s.send(JSON.stringify({ type: 'signal', from: meta.slot, data: msg.data }));
      }
    } else if (meta.role === 'screen') {
      // Route the screen's signal to exactly one phone, by player id tag.
      const targets = this.state.getWebSockets(`p${msg.target}`);
      for (const t of targets) {
        t.send(JSON.stringify({ type: 'signal', from: 'screen', data: msg.data }));
      }
    }
  }

  webSocketClose(ws, code, reason, wasClean) {
    this.handleGone(ws);
  }

  webSocketError(ws) {
    this.handleGone(ws);
  }

  /** A socket dropped — inform the other side so the UI can react. */
  handleGone(ws) {
    const meta = ws.deserializeAttachment() || {};
    if (meta.role === 'controller' && meta.slot) {
      this.sql.exec(`UPDATE players SET connected = 0 WHERE slot = ?`, meta.slot);
      for (const s of this.state.getWebSockets('screen')) {
        try { s.send(JSON.stringify({ type: 'player-left', playerId: meta.slot })); } catch {}
      }
    } else if (meta.role === 'screen') {
      // The host vanished — tell every controller so phones can show "screen offline".
      for (const c of this.state.getWebSockets('controller')) {
        try { c.send(JSON.stringify({ type: 'screen-disconnected' })); } catch {}
      }
    }
  }
}
