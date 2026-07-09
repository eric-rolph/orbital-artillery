/* ==========================================================================
 * Orbital Artillery — DESKTOP SCREEN
 * ==========================================================================
 *
 * Responsibilities:
 *   1. Hold a WebSocket to the signaling Durable Object to obtain the room code
 *      and learn when phones join/leave.
 *   2. Stand up an INDEPENDENT RTCPeerConnection for EACH phone (up to two) and
 *      complete the WebRTC handshake as the *offerer*.
 *   3. Once a phone's RTCDataChannel opens, read its controller input directly
 *      P2P (the server is no longer involved) and drive that player's turret.
 *   4. Own the authoritative game state, run a deterministic fixed-step physics
 *      loop with two gravity wells, and render everything to the canvas.
 *
 * ----- MULTI-PEER ARCHITECTURE (the part worth understanding) --------------
 *
 * One Screen talks to TWO phones at once. WebRTC has no native "one-to-many":
 * each remote phone is its own full-mesh peer connection. So the Screen keeps a
 * `Map<playerId, Peer>` where every entry is a *separate*:
 *
 *        RTCPeerConnection  +  RTCDataChannel  +  its own ICE candidate queue
 *
 * All of these peers are multiplexed over the SINGLE signaling WebSocket. The
 * disambiguator is the player id:
 *
 *   • Outbound: when we emit an SDP offer or ICE candidate for player N, we wrap
 *     it as { type:'signal', target:N, data } so the DO routes it to that one
 *     phone.
 *   • Inbound:  the DO stamps every phone->screen signal with { from:N }, so we
 *     look up peers.get(N) and apply the SDP answer / ICE candidate to the right
 *     connection.
 *
 * Because the Screen is always the offerer and each phone is always the answerer,
 * the roles are fixed and there is no "glare" (no simultaneous offers to reconcile),
 * which keeps the negotiation logic tiny. Each peer trickles ICE independently;
 * candidates that arrive before the remote description is set are queued per-peer
 * and flushed once setRemoteDescription() resolves.
 * ========================================================================== */

'use strict';

// --- ICE configuration. On the same LAN, host candidates connect the two
// devices directly with no relay. A public STUN server is included so it still
// works if the phone and screen are on different subnets / behind NAT. We
// deliberately configure NO TURN server: relaying would put a server back in the
// data path, defeating the zero-server-bandwidth goal. ---
const RTC_CONFIG = {
  iceServers: [{ urls: 'stun:stun.l.google.com:19302' }],
};

// =====================  SIGNALING + PEER MANAGEMENT  ========================

/** @type {Map<number, {pc: RTCPeerConnection, dc: RTCDataChannel|null, ready: boolean, pending: RTCIceCandidateInit[]}>} */
const peers = new Map();
// Players whose P2P handshake failed and who deliver inputs via the DO's
// WebSocket relay instead (see the 'input' case below). A player is "ready"
// if EITHER transport is live.
const relayPlayers = new Set();
let signalSocket = null;
let roomCode = '';

function connectSignaling() {
  const proto = location.protocol === 'https:' ? 'wss' : 'ws';
  // Resume our previous room when we have one (socket flap, or page refresh via
  // sessionStorage) so already-joined phones stay valid — the DO replays their
  // presence and we re-offer. Otherwise the server mints a fresh code.
  const prev = roomCode || sessionStorage.getItem('oa-room') || '';
  const resume = /^[A-Z2-9]{4}$/.test(prev) ? `?code=${prev}` : '';
  signalSocket = new WebSocket(`${proto}://${location.host}/api/screen${resume}`);

  signalSocket.addEventListener('message', (ev) => {
    const msg = JSON.parse(ev.data);
    switch (msg.type) {
      case 'room-created':
        roomCode = msg.code;
        onRoomReady(msg.code);
        break;
      case 'player-joined':
        // A phone connected and was assigned a slot. WE (the screen) initiate
        // the WebRTC offer toward it.
        startPeer(msg.playerId);
        break;
      case 'player-left':
        relayPlayers.delete(msg.playerId);
        teardownPeer(msg.playerId);
        onPlayerDropped(msg.playerId);
        break;
      case 'signal':
        // An SDP answer or ICE candidate from a specific phone.
        handleSignal(msg.from, msg.data);
        break;
      case 'input':
        // RELAY FALLBACK: this player's WebRTC handshake failed, so their
        // inputs arrive through the Durable Object over WebSocket. The first
        // frame doubles as their "ready" announcement.
        if (!msg.from || !msg.data || typeof msg.data !== 'object') break;
        if (!relayPlayers.has(msg.from)) {
          relayPlayers.add(msg.from);
          markPlayerConnection(msg.from, true);
          updateLobby();
          maybeStartMatch();
        }
        handleControllerInput(msg.from, msg.data);
        break;
    }
  });

  signalSocket.addEventListener('close', (ev) => {
    // 4010 = another screen took over this room code. Abandon it so we don't
    // ping-pong takeovers with the other screen; the reconnect below will then
    // mint a fresh room instead.
    if (ev.code === 4010) {
      roomCode = '';
      sessionStorage.removeItem('oa-room');
    }
    // The signaling channel is only needed for handshakes. If it drops after the
    // game is under way, live P2P data channels keep working. We retry so future
    // joins still function.
    setTimeout(connectSignaling, 1500);
  });
}

/**
 * Create a fresh peer connection for `playerId` and send it an SDP offer.
 * The Screen creates the DataChannel here (as the offerer), so it is negotiated
 * inside the very first offer.
 */
async function startPeer(playerId) {
  // If a peer already exists for this slot (e.g. screen refresh / reconnect),
  // discard it before building a new one.
  teardownPeer(playerId);

  const pc = new RTCPeerConnection(RTC_CONFIG);
  const peer = { pc, dc: null, ready: false, pending: [] };
  peers.set(playerId, peer);

  // --- The data channel that will carry this player's controller input. ---
  // ordered+reliable: on a LAN, retransmits are sub-millisecond, and we must not
  // silently drop a FIRE tap. Messages are tiny, so head-of-line blocking is a
  // non-issue in practice.
  const dc = pc.createDataChannel('input', { ordered: true });
  peer.dc = dc;
  wireDataChannel(playerId, dc);

  // Trickle our ICE candidates to THIS phone (targeted by player id).
  pc.addEventListener('icecandidate', (e) => {
    if (e.candidate) sendSignal(playerId, { candidate: e.candidate });
  });

  pc.addEventListener('connectionstatechange', () => {
    const current = peers.get(playerId);
    if (!current || current.pc !== pc) return; // superseded by a newer peer
    // 'disconnected' is TRANSIENT (a Wi-Fi blip during handoff) and usually
    // self-heals within seconds without the data channel ever closing — so it
    // must NOT be treated as terminal. Grey the player out only on genuinely
    // dead states, and restore the indicator when ICE recovers.
    if (['failed', 'closed'].includes(pc.connectionState)) {
      markPlayerConnection(playerId, false);
    } else if (pc.connectionState === 'connected') {
      if (current.dc && current.dc.readyState === 'open') markPlayerConnection(playerId, true);
    }
  });

  // Offer / local-description / ship it.
  const offer = await pc.createOffer();
  await pc.setLocalDescription(offer);
  sendSignal(playerId, { sdp: pc.localDescription });
}

/** Attach handlers to a player's data channel and route its input into the game. */
function wireDataChannel(playerId, dc) {
  dc.addEventListener('open', () => {
    const peer = peers.get(playerId);
    if (peer) peer.ready = true;
    markPlayerConnection(playerId, true);
    updateLobby();
    maybeStartMatch();
  });
  dc.addEventListener('close', () => {
    const peer = peers.get(playerId);
    if (peer) peer.ready = false;
    markPlayerConnection(playerId, false);
    onPlayerDropped(playerId);
  });
  // *** The gameplay hot path. Runs entirely over P2P WebRTC — no server. ***
  dc.addEventListener('message', (ev) => {
    let m;
    try { m = JSON.parse(ev.data); } catch { return; }
    handleControllerInput(playerId, m);
  });
}

/** Apply an inbound SDP answer or ICE candidate to the right peer connection. */
async function handleSignal(playerId, data) {
  const peer = peers.get(playerId);
  // Shape-check the relayed payload — the DO forwards it opaquely, so a buggy
  // or hostile phone could send null/junk that would otherwise throw here.
  if (!peer || !data || typeof data !== 'object') return;
  const pc = peer.pc;

  if (data.sdp) {
    // This is the phone's ANSWER to our offer.
    await pc.setRemoteDescription(new RTCSessionDescription(data.sdp));
    // Now that a remote description exists, flush any ICE that arrived early.
    for (const c of peer.pending.splice(0)) {
      try { await pc.addIceCandidate(new RTCIceCandidate(c)); } catch (err) { console.warn('ICE add failed', err); }
    }
  } else if (data.candidate) {
    if (pc.remoteDescription && pc.remoteDescription.type) {
      try { await pc.addIceCandidate(new RTCIceCandidate(data.candidate)); } catch (err) { console.warn('ICE add failed', err); }
    } else {
      peer.pending.push(data.candidate); // queue until the answer lands
    }
  }
}

/** Wrap a signaling payload for one phone and hand it to the DO to route. */
function sendSignal(playerId, data) {
  if (signalSocket && signalSocket.readyState === WebSocket.OPEN) {
    signalSocket.send(JSON.stringify({ type: 'signal', target: playerId, data }));
  }
}

function teardownPeer(playerId) {
  const peer = peers.get(playerId);
  if (!peer) return;
  try { peer.dc && peer.dc.close(); } catch {}
  try { peer.pc.close(); } catch {}
  peers.delete(playerId);
  markPlayerConnection(playerId, false);
  updateLobby();
}

// =============================  GAME MODEL  =================================

// Fixed logical world. We render this box scaled/letterboxed to any monitor so
// the physics constants stay identical regardless of display resolution.
const WORLD_W = 1600;
const WORLD_H = 900;

// Gravity + ballistics tuning.
const GRAV = 340;            // gravitational strength per unit of (planet radius^2)
const SOFTENING = 10;        // avoids a divide-by-zero singularity at a planet's core
const MIN_SPEED = 170;       // muzzle speed at power = 0
const MAX_SPEED = 640;       // muzzle speed at power = 1
const BARREL_LEN = 46;       // spawn offset from the turret base
const TURRET_HIT_R = 26;     // how close a shot must pass to a turret to score
const PROJ_R = 6;
const FIRE_COOLDOWN = 0.5;   // seconds between shots per player
const PROJ_LIFETIME = 14;    // seconds before an orbiting shot fizzles out
const WIN_SCORE = 3;

const DT = 1 / 120;          // fixed physics timestep
const MAX_STEPS = 8;         // clamp catch-up to avoid a spiral of death

// ---------------------------------------------------------------------------
// THE SOLAR SYSTEM ("Kepler's Duel")
//
// The battlefield is a clockwork heliocentric system, entirely ON RAILS:
// planets ride closed-form ellipses (x = Rx·cos(ωt+φ), y = Ry·sin(ωt+φ)) and
// spin at constant rates, so every position is an analytic function of simTime.
// That buys three things at once:
//   • determinism — no integration error can accumulate in the world itself;
//   • cheap physics — body positions cost two trig calls, no N-body solve;
//   • a truthful preview — the aiming ghost can evaluate the system at
//     (simTime + k·dt) analytically and show where the world WILL be.
//
// MISMATCHED periods (~70s inner vs ~112s outer) make the duel geometry cycle
// through conjunction (clean firing lanes) and opposition (sun in the way —
// slingshot territory) roughly once per round.
// ---------------------------------------------------------------------------

/** Clock the whole system runs on. Advances by DT per physics step. */
let simTime = 0;

// Deterministic PRNG for terrain. The screen is the single authority (phones
// are dumb gamepads), so a per-boot random seed is safe — sim, collision and
// preview all read the same LUTs. A new world every match keeps it fresh.
function mulberry32(seed) {
  let s = seed >>> 0;
  return function () {
    s = (s + 0x6D2B79F5) >>> 0;
    let z = s;
    z = Math.imul(z ^ (z >>> 15), z | 1);
    z ^= z + Math.imul(z ^ (z >>> 7), z | 61);
    return ((z ^ (z >>> 14)) >>> 0) / 4294967296;
  };
}
const WORLD_SEED = (Math.random() * 0xffffffff) >>> 0;

// The sun: fixed at world center, dominant gravity well, kills any projectile
// that touches it. gm is assigned after the planets exist (2.5× the heaviest).
const sun = { x: WORLD_W / 2, y: WORLD_H / 2, r: 66, gm: 0 };
// Passing INSIDE this ring and coming back out alive is a "sun graze" — the
// shot goes white-hot and scores DOUBLE on a hit. (A ring-shaped skill check:
// thread between glory at r≈89 and vaporization at r=72.)
// 1.35 — NOT larger: the inner planet's perihelion surface approaches within
// ~95 of the sun, and the ring must sit safely inside that so a turret can
// never stand inside the bounty zone and mint free double points (the boot
// sweep below asserts this). The bounty is also ENTRY-GATED in the sim: a
// shot must cross INTO the ring in flight before its exit can count.
const GRAZE_R = sun.r * 1.35;

// --- Lumpy terrain: a radial heightfield r(θ) sampled into a LUT. -----------
// Collision against an irregular planet is then: one broad-phase circle test
// (maxR), one atan2, one array lookup — barely dearer than a circle test.
const LUT_N = 128;

function makeTerrain(rng, baseR, roughness) {
  // Sum a few low-order harmonics for continents/ridges. Roughness is kept
  // modest (±~10% of baseR) so orbit clearances stay provable below.
  const harmonics = [];
  for (let h = 0; h < 4; h++) {
    harmonics.push({
      k: 2 + h + Math.floor(rng() * 3),            // angular frequency
      amp: (roughness * baseR) * (0.4 + rng() * 0.6) / (h + 1),
      ph: rng() * Math.PI * 2,
    });
  }
  const lut = new Float32Array(LUT_N);
  let maxR = 0;
  for (let i = 0; i < LUT_N; i++) {
    const th = (i / LUT_N) * Math.PI * 2;
    let r = baseR;
    for (const hm of harmonics) r += hm.amp * Math.sin(hm.k * th + hm.ph);
    lut[i] = r;
    if (r > maxR) maxR = r;
  }
  return { lut, maxR };
}

/** Surface radius at a LOCAL (unrotated) angle. */
function lutRadius(pl, localAngle) {
  let idx = Math.round((localAngle / (Math.PI * 2)) * LUT_N) % LUT_N;
  if (idx < 0) idx += LUT_N;
  return pl.lut[idx];
}

function makePlanet(cfg) {
  const rng = mulberry32(WORLD_SEED ^ Math.imul(cfg.id + 1, 0x9e3779b9));
  const { lut, maxR } = makeTerrain(rng, cfg.baseR, 0.10);

  // Cache the silhouette as a Path2D in LOCAL coordinates; rendering just
  // translates + rotates it, so planet spin is free at draw time.
  const path = new Path2D();
  for (let i = 0; i < LUT_N; i++) {
    const th = (i / LUT_N) * Math.PI * 2;
    const px = Math.cos(th) * lut[i], py = Math.sin(th) * lut[i];
    if (i === 0) path.moveTo(px, py); else path.lineTo(px, py);
  }
  path.closePath();

  // Dark surface blotches (drawn inside the rotating frame) — they're what
  // makes the spin VISIBLE, alongside the lumpy silhouette.
  const blotches = [];
  for (let i = 0; i < 5; i++) {
    const a = rng() * Math.PI * 2, d = rng() * cfg.baseR * 0.65;
    blotches.push({ x: Math.cos(a) * d, y: Math.sin(a) * d, r: cfg.baseR * (0.12 + rng() * 0.2) });
  }

  return {
    ...cfg,
    w: (Math.PI * 2) / cfg.period,   // orbital angular rate
    gm: GRAV * cfg.baseR * cfg.baseR,
    lut, maxR, path, blotches,
    x: 0, y: 0, rot: 0,              // filled by updateBodies() every step
  };
}

// Inner world orbits fast (~70s), outer slow (~112s); they counter-rotate.
// Ellipses are x-stretched to exploit the 16:9 world. Radii/sizes are chosen
// so the two planets can NEVER touch (verified by the boot-time sweep below)
// and the inner one always clears the sun's corona.
const planets = [
  makePlanet({ id: 0, baseR: 66, Rx: 280, Ry: 175, period: 70,  phi: 0,       spin: +(Math.PI * 2) / 85, color: '#6b8cff', glow: '#3a56c9' }),
  makePlanet({ id: 1, baseR: 72, Rx: 470, Ry: 345, period: 112, phi: Math.PI, spin: -(Math.PI * 2) / 95, color: '#ff9d5c', glow: '#c9622a' }),
];
sun.gm = 2.5 * Math.max(...planets.map((p) => p.gm));

/** Closed-form planet center at time t. */
function planetPosAt(pl, t) {
  const a = pl.w * t + pl.phi;
  return { x: sun.x + pl.Rx * Math.cos(a), y: sun.y + pl.Ry * Math.sin(a) };
}

/** Closed-form planet velocity at time t (derivative of the ellipse). */
function planetVelAt(pl, t) {
  const a = pl.w * t + pl.phi;
  return { vx: -pl.Rx * pl.w * Math.sin(a), vy: pl.Ry * pl.w * Math.cos(a) };
}

/** Does point (x,y) at time t penetrate planet pl's terrain (padded by pad)? */
function hitsPlanet(pl, x, y, t, pad) {
  const c = planetPosAt(pl, t);
  const dx = x - c.x, dy = y - c.y;
  const d2 = dx * dx + dy * dy;
  const broad = pl.maxR + pad;
  if (d2 > broad * broad) return false;                       // broad phase
  const r = lutRadius(pl, Math.atan2(dy, dx) - pl.spin * t) + pad; // narrow: LUT
  return d2 <= r * r;
}

// Two turrets, one per player, anchored on the surface of "their" planet.
// anchor is the LOCAL angle on the planet — rotation carries the turret
// around the world, which is half the gameplay of this design.
const turrets = [
  makeTurret(1, planets[0], -Math.PI * 0.62, -0.15, '#ff6b6b'), // Player 1, inner world
  makeTurret(2, planets[1], -Math.PI * 0.38, Math.PI + 0.15, '#4dd0ff'), // Player 2, outer world
];

function makeTurret(id, planet, anchorAngle, defaultAim, color) {
  return {
    id, planet, color,
    x: 0, y: 0,            // world position — recomputed each step (planet moves & spins)
    anchorAngle,           // local surface angle
    anchorR: lutRadius(planet, anchorAngle), // surface radius at that angle
    aim: defaultAim,       // barrel angle (world radians — the phone dial is world-space)
    power: 0.5,            // 0..1
    score: 0,
    cooldown: 0,
    connected: false,
    flash: 0,              // brief hit animation timer
  };
}

/** Recompute every on-rails body + everything riding one, for `simTime`. */
function updateBodies() {
  for (const pl of planets) {
    const c = planetPosAt(pl, simTime);
    pl.x = c.x; pl.y = c.y;
    pl.rot = pl.spin * simTime;
  }
  for (const t of turrets) {
    const a = t.anchorAngle + t.planet.rot;
    t.x = t.planet.x + Math.cos(a) * t.anchorR;
    t.y = t.planet.y + Math.sin(a) * t.anchorR;
  }
}

// Boot-time safety sweep: prove the two planets can never overlap at ANY pair
// of orbital angles (a superset of what mismatched periods can produce).
(function verifyOrbitClearance() {
  const need = planets[0].maxR + planets[1].maxR + 4;
  let worst = Infinity;
  for (let i = 0; i < 128; i++) {
    const a1 = (i / 128) * Math.PI * 2;
    const p1x = sun.x + planets[0].Rx * Math.cos(a1), p1y = sun.y + planets[0].Ry * Math.sin(a1);
    for (let j = 0; j < 128; j++) {
      const a2 = (j / 128) * Math.PI * 2;
      const dx = p1x - (sun.x + planets[1].Rx * Math.cos(a2));
      const dy = p1y - (sun.y + planets[1].Ry * Math.sin(a2));
      const d = Math.hypot(dx, dy);
      if (d < worst) worst = d;
    }
  }
  if (worst < need) console.warn(`Orbit clearance violated: min ${worst.toFixed(1)} < needed ${need.toFixed(1)}`);
  // The graze bounty's anti-exploit invariant: no planet surface (hence no
  // turret) may ever dip inside the graze ring.
  for (const p of planets) {
    const closest = Math.min(p.Rx, p.Ry) - p.maxR;
    if (closest <= GRAZE_R + 4) console.warn(`Graze-ring clearance violated: planet ${p.id} approaches ${closest.toFixed(1)} <= ${(GRAZE_R + 4).toFixed(1)}`);
  }
})();

/** Render-only effects (vaporization flashes etc.) — never touch the sim. */
let fx = [];

/** @type {{x:number,y:number,vx:number,vy:number,owner:number,life:number,muzzle:number,inGraze:boolean,boosted:boolean,trail:{x:number,y:number}[]}[]} */
let projectiles = [];

// High-level state machine for the screen.
let phase = 'lobby'; // 'lobby' | 'countdown' | 'playing' | 'paused' | 'gameover'
let countdown = 0;
let bannerTimer = 0;

// ---- Gravity field shared by the live sim AND the aim-preview ghost. -------
// Pass a future t and the field is evaluated where the planets WILL be —
// that's what keeps the trajectory preview truthful on a moving battlefield.
function gravityAt(x, y, t = simTime) {
  // Sun (fixed).
  let dx = sun.x - x, dy = sun.y - y;
  let r2 = dx * dx + dy * dy + SOFTENING * SOFTENING;
  let invR = 1 / Math.sqrt(r2);
  let a = sun.gm / r2;
  let ax = a * dx * invR, ay = a * dy * invR;
  // Planets (on rails at time t).
  for (const p of planets) {
    const c = planetPosAt(p, t);
    dx = c.x - x; dy = c.y - y;
    r2 = dx * dx + dy * dy + SOFTENING * SOFTENING;
    invR = 1 / Math.sqrt(r2);
    a = p.gm / r2; // magnitude = GM / r²
    ax += a * dx * invR;
    ay += a * dy * invR;
  }
  return { ax, ay };
}

// =============================  INPUT  =====================================

/**
 * Handle one controller packet — already parsed — from EITHER transport
 * (P2P data channel, or the DO relay for players whose handshake failed).
 * Packet shapes (kept tiny for latency):
 *   { t:'aim',   a:<radians> }
 *   { t:'power', p:<0..1> }
 *   { t:'fire' }
 */
function handleControllerInput(playerId, msg) {
  if (!msg || typeof msg !== 'object') return;
  const turret = turrets[playerId - 1];
  if (!turret) return;

  switch (msg.t) {
    // Number.isFinite (not typeof) — NaN/Infinity sail through a typeof check
    // AND through Math.min/Math.max clamping, then poison the physics.
    case 'aim':
      if (Number.isFinite(msg.a)) turret.aim = msg.a;
      break;
    case 'power':
      if (Number.isFinite(msg.p)) turret.power = Math.max(0, Math.min(1, msg.p));
      break;
    case 'fire':
      fire(turret);
      break;
  }
}

function fire(turret) {
  if (phase !== 'playing') return;
  if (turret.cooldown > 0) return;
  turret.cooldown = FIRE_COOLDOWN;

  const dir = turret.aim;
  const speed = MIN_SPEED + turret.power * (MAX_SPEED - MIN_SPEED);
  // Spawn at the barrel tip, safely outside the planet surface.
  const sx = turret.x + Math.cos(dir) * BARREL_LEN;
  const sy = turret.y + Math.sin(dir) * BARREL_LEN;
  // The shot inherits its launch platform's orbital velocity (a real frame
  // effect, ~25 u/s) — the trajectory preview accounts for it identically.
  const pv = planetVelAt(turret.planet, simTime);
  const vx = Math.cos(dir) * speed + pv.vx;
  const vy = Math.sin(dir) * speed + pv.vy;
  projectiles.push({
    x: sx, y: sy, vx, vy,
    owner: turret.id,
    life: PROJ_LIFETIME,
    muzzle: Math.hypot(vx, vy), // reference speed for flavor/debug HUDs
    // Graze bookkeeping — initialized from the ACTUAL spawn position, and
    // entry-gated: only a shot that crosses INTO the ring during flight can
    // earn the bounty on its way out. Both guards exist so a turret standing
    // near perihelion can never mint free double points.
    inGraze: (sx - sun.x) ** 2 + (sy - sun.y) ** 2 < GRAZE_R * GRAZE_R,
    enteredGraze: false,
    boosted: false,             // survived a sun graze → worth double
    trail: [],
  });
}

// =============================  SIMULATION  ================================

let lastTime = 0;
let accumulator = 0;

function frame(now) {
  requestAnimationFrame(frame);
  if (!lastTime) lastTime = now;
  let dtReal = (now - lastTime) / 1000;
  lastTime = now;
  if (dtReal > 0.25) dtReal = 0.25; // tab was backgrounded — don't fast-forward wildly

  // Fixed-timestep integration for deterministic, resolution-independent physics.
  accumulator += dtReal;
  let steps = 0;
  while (accumulator >= DT && steps < MAX_STEPS) {
    stepWorld(DT);
    accumulator -= DT;
    steps++;
  }
  if (steps === MAX_STEPS) accumulator = 0; // we fell behind; drop the backlog

  render();
}

function stepWorld(dt) {
  // The solar system runs on rails in EVERY phase — the world keeps turning
  // in the lobby, during countdowns and pauses. Only gameplay consequences
  // (projectiles, scoring) are gated on 'playing' below.
  simTime += dt;
  updateBodies();

  // Timers (cooldowns, flashes, banners, fx) advance in every phase.
  for (const t of turrets) {
    if (t.cooldown > 0) t.cooldown = Math.max(0, t.cooldown - dt);
    if (t.flash > 0) t.flash = Math.max(0, t.flash - dt);
  }
  for (const f of fx) f.life -= dt;
  fx = fx.filter((f) => f.life > 0);
  if (bannerTimer > 0) {
    bannerTimer -= dt;
    if (bannerTimer <= 0) hideBanner();
  }

  if (phase === 'countdown') {
    countdown -= dt;
    if (countdown <= 0) { phase = 'playing'; }
    return;
  }
  if (phase !== 'playing') return;

  // Integrate every projectile under the sun + both (moving) planets.
  for (const p of projectiles) {
    const g = gravityAt(p.x, p.y);
    p.vx += g.ax * dt;
    p.vy += g.ay * dt;
    p.x += p.vx * dt;
    p.y += p.vy * dt;
    p.life -= dt;

    // Sun-graze bounty: dip inside the graze ring and come back out alive and
    // the shot goes white-hot — a hit is worth DOUBLE. Entry-gated: the shot
    // must have crossed INTO the ring during flight (not merely started near
    // the sun) before an exit crossing can qualify.
    const dxs = p.x - sun.x, dys = p.y - sun.y;
    const insideGraze = dxs * dxs + dys * dys < GRAZE_R * GRAZE_R;
    if (!p.inGraze && insideGraze) p.enteredGraze = true;
    if (p.inGraze && !insideGraze && p.enteredGraze) p.boosted = true;
    p.inGraze = insideGraze;

    p.trail.push({ x: p.x, y: p.y });
    if (p.trail.length > 40) p.trail.shift();
  }

  // Resolve collisions and keep survivors.
  const survivors = [];
  for (const p of projectiles) {
    const outcome = classifyProjectile(p);
    if (outcome === 'alive') { survivors.push(p); continue; }
    if (outcome.reason === 'sun') {
      // Vaporized — a bright render-only flash at the point of death.
      fx.push({ x: p.x, y: p.y, r: 10, life: 0.5, maxLife: 0.5, type: 'vaporize' });
      continue;
    }
    if (outcome.hitTurret) {
      // Score EXACTLY one hit per step. onTurretHit clears the field and moves
      // the phase machine (possibly to gameover) — processing further
      // collisions after it could erase a win or double-score, and assigning
      // `projectiles = survivors` below would resurrect the cleared shots.
      onTurretHit(outcome.hitTurret, p);
      return;
    }
    // otherwise it hit a planet, flew off-world, or fizzled — just remove it.
  }
  projectiles = survivors;
}

/** Returns 'alive' or an outcome object describing why the projectile ended. */
function classifyProjectile(p) {
  if (p.life <= 0) return { reason: 'fizzle' };
  // Off into deep space?
  if (p.x < -400 || p.x > WORLD_W + 400 || p.y < -400 || p.y > WORLD_H + 400) {
    return { reason: 'oob' };
  }
  // Swallowed by the sun?
  {
    const dx = sun.x - p.x, dy = sun.y - p.y;
    if (dx * dx + dy * dy <= (sun.r + PROJ_R) * (sun.r + PROJ_R)) return { reason: 'sun' };
  }
  // Hit a planet's (lumpy, rotating) terrain?
  for (const pl of planets) {
    if (hitsPlanet(pl, p.x, p.y, simTime, PROJ_R)) return { reason: 'planet' };
  }
  // Hit a turret? (Only the enemy's — see the owner check.)
  for (const t of turrets) {
    if (t.id === p.owner) continue; // your shot can't score on your own turret while leaving the muzzle...
    const dx = t.x - p.x, dy = t.y - p.y;
    if (dx * dx + dy * dy <= (TURRET_HIT_R + PROJ_R) * (TURRET_HIT_R + PROJ_R)) {
      return { reason: 'turret', hitTurret: t };
    }
  }
  return 'alive';
}

function onTurretHit(targetTurret, proj) {
  if (phase !== 'playing') return; // never score outside live play (defense-in-depth)
  const shooter = turrets[proj.owner - 1];
  if (!shooter) return;
  const points = proj.boosted ? 2 : 1; // sun-grazed shots score double
  shooter.score += points;
  targetTurret.flash = 0.8;
  projectiles = []; // clear the field for a clean restart of the volley

  if (shooter.score >= WIN_SCORE) {
    phase = 'gameover';
    showBanner(`PLAYER ${shooter.id} WINS`, shooter.color, 5, () => resetMatch());
  } else {
    if (proj.boosted) showBanner(`SLINGSHOT! PLAYER ${shooter.id} +2`, '#ffd166', 1.4);
    else showBanner(`PLAYER ${shooter.id} SCORES`, shooter.color, 1.2);
    // brief pause then continue
    phase = 'countdown';
    countdown = 1.2;
  }
}

function resetMatch() {
  for (const t of turrets) { t.score = 0; t.cooldown = 0; t.flash = 0; }
  projectiles = [];
  // Don't restart into a dead room: if a player dropped during the WIN banner,
  // fall back to the lobby — maybeStartMatch() restarts when both return.
  if (!(playerReady(1) && playerReady(2))) {
    phase = 'lobby';
    lobbyEl.classList.remove('hidden');
    updateLobby();
    return;
  }
  startCountdown();
}

function startCountdown() {
  phase = 'countdown';
  countdown = 3;
}

// =============================  RENDERING  =================================

const canvas = document.getElementById('game');
const ctx = canvas.getContext('2d');
const view = { scale: 1, ox: 0, oy: 0 };

// Pre-generated parallax starfield (world coordinates).
const stars = [];
(function seedStars() {
  // Deterministic-ish scatter; visuals only, so Math.random is fine here.
  for (let i = 0; i < 220; i++) {
    stars.push({
      x: Math.random() * WORLD_W,
      y: Math.random() * WORLD_H,
      r: Math.random() * 1.6 + 0.3,
      a: Math.random() * 0.6 + 0.2,
    });
  }
})();

function resize() {
  const dpr = window.devicePixelRatio || 1;
  const cw = canvas.clientWidth || window.innerWidth;
  const ch = canvas.clientHeight || window.innerHeight;
  canvas.width = Math.round(cw * dpr);
  canvas.height = Math.round(ch * dpr);
  const s = Math.min(canvas.width / WORLD_W, canvas.height / WORLD_H);
  view.scale = s;
  view.ox = (canvas.width - WORLD_W * s) / 2;
  view.oy = (canvas.height - WORLD_H * s) / 2;
}
window.addEventListener('resize', resize);

function render() {
  // 1) Clear the whole backing store (device pixels).
  ctx.setTransform(1, 0, 0, 1, 0, 0);
  ctx.fillStyle = '#03040a';
  ctx.fillRect(0, 0, canvas.width, canvas.height);

  // 2) Switch into world space for everything else.
  ctx.setTransform(view.scale, 0, 0, view.scale, view.ox, view.oy);

  // Subtle world vignette so the play area reads against the letterbox bars.
  const grad = ctx.createRadialGradient(WORLD_W / 2, WORLD_H * 0.3, 100, WORLD_W / 2, WORLD_H / 2, WORLD_W * 0.75);
  grad.addColorStop(0, 'rgba(30,44,80,0.55)');
  grad.addColorStop(1, 'rgba(3,4,10,0.9)');
  ctx.fillStyle = grad;
  ctx.fillRect(0, 0, WORLD_W, WORLD_H);

  // Stars.
  for (const s of stars) {
    ctx.globalAlpha = s.a;
    ctx.fillStyle = '#cdd8ff';
    ctx.beginPath();
    ctx.arc(s.x, s.y, s.r, 0, Math.PI * 2);
    ctx.fill();
  }
  ctx.globalAlpha = 1;

  // Orbit rings — faint dashed ellipses so the clockwork is readable at a glance.
  ctx.save();
  ctx.strokeStyle = 'rgba(205,216,255,0.09)';
  ctx.lineWidth = 1.5;
  ctx.setLineDash([4, 10]);
  for (const p of planets) {
    ctx.beginPath();
    ctx.ellipse(sun.x, sun.y, p.Rx, p.Ry, 0, 0, Math.PI * 2);
    ctx.stroke();
  }
  ctx.restore();

  drawSun();

  // Ghost outlines: where each planet WILL be in 1.5s — pairs with the
  // time-true trajectory preview so leading a moving world is one glance.
  ctx.save();
  ctx.setLineDash([5, 7]);
  ctx.lineWidth = 2;
  for (const p of planets) {
    const c = planetPosAt(p, simTime + 1.5);
    ctx.strokeStyle = p.glow;
    ctx.globalAlpha = 0.22;
    ctx.beginPath();
    ctx.arc(c.x, c.y, p.baseR, 0, Math.PI * 2);
    ctx.stroke();
  }
  ctx.restore();
  ctx.globalAlpha = 1;

  // Planets: cached lumpy silhouette, rotated — spin comes free at draw time.
  for (const p of planets) drawPlanet(p);

  // Aim-trajectory previews (only while playing) — a ghost shot per player.
  if (phase === 'playing' || phase === 'countdown') {
    for (const t of turrets) if (t.connected) drawTrajectoryPreview(t);
  }

  // Turrets.
  for (const t of turrets) drawTurret(t);

  // Projectiles with trails.
  for (const p of projectiles) drawProjectile(p);

  // Render-only effects (sun vaporization flashes).
  for (const f of fx) {
    const k = f.life / f.maxLife; // 1 → 0
    ctx.globalAlpha = k * 0.9;
    ctx.fillStyle = '#fff2c4';
    ctx.beginPath();
    ctx.arc(f.x, f.y, f.r + (1 - k) * 26, 0, Math.PI * 2);
    ctx.fill();
  }
  ctx.globalAlpha = 1;

  // Countdown number.
  if (phase === 'countdown' && countdown > 0) {
    const n = Math.ceil(countdown);
    ctx.globalAlpha = 0.85;
    ctx.fillStyle = '#e8eefc';
    ctx.font = '800 160px system-ui, sans-serif';
    ctx.textAlign = 'center';
    ctx.textBaseline = 'middle';
    ctx.fillText(String(n), WORLD_W / 2, WORLD_H / 2);
    ctx.globalAlpha = 1;
  }
}

function drawSun() {
  const pulse = 1 + 0.05 * Math.sin(simTime * 2.1);
  // Corona: layered soft glow out to ~2.2r.
  ctx.save();
  const cg = ctx.createRadialGradient(sun.x, sun.y, sun.r * 0.4, sun.x, sun.y, sun.r * 2.3 * pulse);
  cg.addColorStop(0, 'rgba(255,214,102,0.85)');
  cg.addColorStop(0.35, 'rgba(255,160,70,0.35)');
  cg.addColorStop(1, 'rgba(255,120,40,0)');
  ctx.fillStyle = cg;
  ctx.beginPath();
  ctx.arc(sun.x, sun.y, sun.r * 2.3 * pulse, 0, Math.PI * 2);
  ctx.fill();

  // The graze ring — the double-points skill check, taught by drawing it.
  ctx.setLineDash([3, 9]);
  ctx.strokeStyle = 'rgba(255,214,102,0.28)';
  ctx.lineWidth = 1.5;
  ctx.beginPath();
  ctx.arc(sun.x, sun.y, GRAZE_R, 0, Math.PI * 2);
  ctx.stroke();
  ctx.setLineDash([]);

  // Core.
  const g = ctx.createRadialGradient(sun.x - sun.r * 0.25, sun.y - sun.r * 0.25, sun.r * 0.1, sun.x, sun.y, sun.r);
  g.addColorStop(0, '#fffbe8');
  g.addColorStop(0.5, '#ffcf5c');
  g.addColorStop(1, '#ff8a3c');
  ctx.fillStyle = g;
  ctx.beginPath();
  ctx.arc(sun.x, sun.y, sun.r, 0, Math.PI * 2);
  ctx.fill();
  ctx.restore();
}

function drawPlanet(p) {
  ctx.save();
  ctx.translate(p.x, p.y);
  ctx.rotate(p.rot); // the whole local frame spins — silhouette AND features
  ctx.shadowColor = p.glow;
  ctx.shadowBlur = 50;
  const g = ctx.createRadialGradient(-p.baseR * 0.3, -p.baseR * 0.3, p.baseR * 0.1, 0, 0, p.maxR);
  g.addColorStop(0, '#ffffff');
  g.addColorStop(0.25, p.color);
  g.addColorStop(1, p.glow);
  ctx.fillStyle = g;
  ctx.fill(p.path);
  ctx.shadowBlur = 0;
  // Surface blotches, clipped to the silhouette — these make the spin visible.
  ctx.clip(p.path);
  ctx.globalAlpha = 0.22;
  ctx.fillStyle = '#000';
  for (const b of p.blotches) {
    ctx.beginPath();
    ctx.arc(b.x, b.y, b.r, 0, Math.PI * 2);
    ctx.fill();
  }
  ctx.globalAlpha = 1;
  ctx.restore();
}

function drawTurret(t) {
  const owner = t.color;
  // Base dome.
  ctx.save();
  if (t.flash > 0) {
    ctx.shadowColor = '#fff';
    ctx.shadowBlur = 40 * (t.flash / 0.8);
  }
  ctx.fillStyle = owner;
  ctx.beginPath();
  ctx.arc(t.x, t.y, 15, 0, Math.PI * 2);
  ctx.fill();
  ctx.restore();

  // Barrel pointing along the current aim.
  ctx.save();
  ctx.translate(t.x, t.y);
  ctx.rotate(t.aim);
  ctx.fillStyle = owner;
  ctx.fillRect(0, -5, BARREL_LEN, 10);
  // muzzle cap
  ctx.fillStyle = '#fff';
  ctx.fillRect(BARREL_LEN - 6, -5, 6, 10);
  ctx.restore();

  // Power ring under the turret.
  const ringR = 26;
  ctx.beginPath();
  ctx.strokeStyle = 'rgba(255,255,255,0.15)';
  ctx.lineWidth = 4;
  ctx.arc(t.x, t.y, ringR, 0, Math.PI * 2);
  ctx.stroke();
  ctx.beginPath();
  ctx.strokeStyle = owner;
  ctx.lineWidth = 4;
  ctx.arc(t.x, t.y, ringR, -Math.PI / 2, -Math.PI / 2 + Math.PI * 2 * t.power);
  ctx.stroke();
}

function drawProjectile(p) {
  // Sun-grazed shots burn white-gold — everyone on the couch sees the stakes.
  const tint = p.boosted ? '#ffd166' : (p.owner === 1 ? '#ff6b6b' : '#4dd0ff');
  // Trail.
  for (let i = 0; i < p.trail.length; i++) {
    const pt = p.trail[i];
    const a = i / p.trail.length;
    ctx.globalAlpha = a * (p.boosted ? 0.75 : 0.5);
    ctx.fillStyle = tint;
    ctx.beginPath();
    ctx.arc(pt.x, pt.y, PROJ_R * a * (p.boosted ? 1.4 : 1), 0, Math.PI * 2);
    ctx.fill();
  }
  ctx.globalAlpha = 1;
  // Head.
  ctx.save();
  ctx.shadowColor = tint;
  ctx.shadowBlur = p.boosted ? 30 : 18;
  ctx.fillStyle = '#fff';
  ctx.beginPath();
  ctx.arc(p.x, p.y, PROJ_R * (p.boosted ? 1.25 : 1), 0, Math.PI * 2);
  ctx.fill();
  ctx.restore();
}

/**
 * TIME-TRUE trajectory preview: the ghost shot is integrated against the
 * system as it WILL be — gravity and collision both evaluate planet positions
 * and rotations at (simTime + k·dt), which is exact because the bodies are on
 * closed-form rails. Dots turn gold from the step the shot would earn the
 * sun-graze double bounty, teaching the mechanic without a word of rules.
 */
function drawTrajectoryPreview(t) {
  const dir = t.aim;
  const speed = MIN_SPEED + t.power * (MAX_SPEED - MIN_SPEED);
  const pv = planetVelAt(t.planet, simTime); // same inheritance as fire()
  let x = t.x + Math.cos(dir) * BARREL_LEN;
  let y = t.y + Math.sin(dir) * BARREL_LEN;
  let vx = Math.cos(dir) * speed + pv.vx;
  let vy = Math.sin(dir) * speed + pv.vy;

  // PARITY IS SACRED. This ghost must be the sim, not an approximation of it:
  // identical integrator step (DT, not a coarser one — semi-implicit Euler's
  // error is O(dt), and near-sun slingshots amplify any mismatch into a
  // different deflection), identical collision pads (PROJ_R everywhere),
  // identical graze bookkeeping (spawn-init + entry-gated), identical OOB
  // margins, and it even ends on the enemy turret like a real hit would.
  // 200 substeps of DT = the same 1.67s lookahead; draw every 6th.
  let inGraze = (x - sun.x) ** 2 + (y - sun.y) ** 2 < GRAZE_R * GRAZE_R;
  let enteredGraze = false, boosted = false;
  const enemy = turrets[t.id === 1 ? 1 : 0];
  const STEPS = 200;
  for (let i = 0; i < STEPS; i++) {
    const tf = simTime + (i + 1) * DT; // the future moment this substep lands on
    const g = gravityAt(x, y, tf);
    vx += g.ax * DT; vy += g.ay * DT;
    x += vx * DT; y += vy * DT;

    // Dies in the sun? (Same padded test as classifyProjectile.)
    const dxs = x - sun.x, dys = y - sun.y;
    const ds2 = dxs * dxs + dys * dys;
    if (ds2 <= (sun.r + PROJ_R) * (sun.r + PROJ_R)) break;
    // Same entry-gated graze bookkeeping as the live sim.
    const ig = ds2 < GRAZE_R * GRAZE_R;
    if (!inGraze && ig) enteredGraze = true;
    if (inGraze && !ig && enteredGraze) boosted = true;
    inGraze = ig;

    // Ends on the enemy turret (at ITS future position), like the sim.
    const ec = planetPosAt(enemy.planet, tf);
    const ea = enemy.anchorAngle + enemy.planet.spin * tf;
    const ex = ec.x + Math.cos(ea) * enemy.anchorR - x;
    const ey = ec.y + Math.sin(ea) * enemy.anchorR - y;
    if (ex * ex + ey * ey <= (TURRET_HIT_R + PROJ_R) * (TURRET_HIT_R + PROJ_R)) break;

    // Stop at terrain — planets where they'll be at tf, sim's pad.
    let blocked = false;
    for (const pl of planets) {
      if (hitsPlanet(pl, x, y, tf, PROJ_R)) { blocked = true; break; }
    }
    if (blocked || x < -400 || x > WORLD_W + 400 || y < -400 || y > WORLD_H + 400) break;

    if (i % 6 === 0) {
      ctx.fillStyle = boosted ? '#ffd166' : t.color;
      ctx.globalAlpha = (boosted ? 0.8 : 0.5) * (1 - i / STEPS);
      ctx.beginPath();
      ctx.arc(x, y, boosted ? 3.6 : 3, 0, Math.PI * 2);
      ctx.fill();
    }
  }
  ctx.globalAlpha = 1;
}

// =============================  LOBBY / HUD DOM  ===========================

const lobbyEl = document.getElementById('lobby');
const hudEl = document.getElementById('hud');
const hudCodeEl = document.getElementById('hud-code');
const bannerEl = document.getElementById('banner');
const bannerMsgEl = document.getElementById('banner-msg');

function onRoomReady(code) {
  // Remember the room across a page refresh so we resume it (see connectSignaling)
  // instead of stranding already-joined phones in an unhosted room.
  try { sessionStorage.setItem('oa-room', code); } catch {}
  document.getElementById('room-code').textContent = code;
  document.getElementById('hud-code-val').textContent = code;
  document.getElementById('join-host').textContent = location.host;

  // Deep link the phone straight into this room; render a QR if the lib loaded.
  const joinUrl = `${location.origin}/controller.html?code=${code}`;
  const qrEl = document.getElementById('qr');
  if (window.QRCode && typeof window.QRCode.toCanvas === 'function') {
    const c = document.createElement('canvas');
    window.QRCode.toCanvas(c, joinUrl, { width: 176, margin: 1 }, (err) => {
      if (err) { qrEl.classList.add('empty'); return; }
      qrEl.innerHTML = '';
      qrEl.appendChild(c);
      qrEl.classList.remove('empty');
    });
  } else {
    qrEl.classList.add('empty');
  }
}

function markPlayerConnection(playerId, up) {
  const t = turrets[playerId - 1];
  if (t) t.connected = up;
  const el = document.getElementById('conn-' + playerId);
  if (el) el.style.color = up ? (playerId === 1 ? '#ff6b6b' : '#4dd0ff') : '#55607a';
}

/** A player is ready when EITHER transport is live: P2P channel or DO relay. */
function playerReady(id) {
  return !!peers.get(id)?.ready || relayPlayers.has(id);
}

/** Reflect join/ready state in the lobby player slots. */
function updateLobby() {
  for (let id = 1; id <= 2; id++) {
    const slot = document.getElementById('slot-' + id);
    if (!slot) continue;
    let state = 'empty', status = 'waiting…';
    if (playerReady(id)) { state = 'ready'; status = relayPlayers.has(id) ? 'ready ✔ (relay)' : 'ready ✔'; }
    else if (peers.get(id)) { state = 'joined'; status = 'connecting…'; }
    slot.dataset.state = state;
    slot.querySelector('.status').textContent = status;
  }
}

/** Begin the match once BOTH players have a live input transport. */
function maybeStartMatch() {
  if (phase !== 'lobby' && phase !== 'paused') return;
  if (playerReady(1) && playerReady(2)) {
    lobbyEl.classList.add('hidden');
    hudEl.classList.add('show');
    hudCodeEl.classList.add('show');
    if (phase === 'paused') { phase = 'playing'; hideBanner(); }
    else { startCountdown(); }
  }
}

/** A player's channel dropped. Pause the match and wait for them to return. */
function onPlayerDropped(playerId) {
  updateLobby();
  if (phase === 'playing' || phase === 'countdown') {
    phase = 'paused';
    // Clear live shots: the solar system keeps orbiting while paused, so a
    // frozen projectile could be swept into a turret and score on resume.
    projectiles = [];
    showBanner(`PLAYER ${playerId} DISCONNECTED`, '#ffd166', 9999);
  }
}

let bannerCallback = null;
function showBanner(text, color, seconds, cb) {
  bannerMsgEl.textContent = text;
  bannerMsgEl.style.color = color || '#fff';
  bannerEl.classList.add('show');
  bannerTimer = seconds;
  bannerCallback = cb || null;
}
function hideBanner() {
  bannerEl.classList.remove('show');
  const cb = bannerCallback; bannerCallback = null;
  if (cb) cb();
}

// Keep the numeric scoreboard in sync each frame (cheap DOM writes only on change).
let lastScores = [-1, -1];
function syncScores() {
  for (let i = 0; i < 2; i++) {
    if (turrets[i].score !== lastScores[i]) {
      lastScores[i] = turrets[i].score;
      document.getElementById('score-' + (i + 1)).textContent = turrets[i].score;
    }
  }
  requestAnimationFrame(syncScores);
}

// =============================  BOOT  =====================================

resize();
updateBodies(); // position the solar system before the first render tick
connectSignaling();
requestAnimationFrame(frame);
requestAnimationFrame(syncScores);
