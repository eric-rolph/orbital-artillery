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
let signalSocket = null;
let roomCode = '';

function connectSignaling() {
  const proto = location.protocol === 'https:' ? 'wss' : 'ws';
  signalSocket = new WebSocket(`${proto}://${location.host}/api/screen`);

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
        teardownPeer(msg.playerId);
        break;
      case 'signal':
        // An SDP answer or ICE candidate from a specific phone.
        handleSignal(msg.from, msg.data);
        break;
    }
  });

  signalSocket.addEventListener('close', () => {
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
    if (['failed', 'disconnected', 'closed'].includes(pc.connectionState)) {
      markPlayerConnection(playerId, false);
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
  dc.addEventListener('message', (ev) => handleControllerInput(playerId, ev.data));
}

/** Apply an inbound SDP answer or ICE candidate to the right peer connection. */
async function handleSignal(playerId, data) {
  const peer = peers.get(playerId);
  if (!peer) return;
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

// Two celestial bodies with independent gravity wells.
const planets = [
  { x: 470, y: 520, r: 92, color: '#6b8cff', glow: '#3a56c9' },
  { x: 1150, y: 380, r: 116, color: '#ff9d5c', glow: '#c9622a' },
];
// Precompute GM (= G · mass, with mass ∝ r²) for each planet.
for (const p of planets) p.gm = GRAV * p.r * p.r;

// Two turrets, one per player, anchored on the surface of "their" planet.
// anchor is the angle (from the planet center) at which the turret sits.
const turrets = [
  makeTurret(1, planets[0], -Math.PI * 0.62, -0.15, '#ff6b6b'), // Player 1 on the blue planet
  makeTurret(2, planets[1], -Math.PI * 0.38, Math.PI + 0.15, '#4dd0ff'), // Player 2 on the orange planet
];

function makeTurret(id, planet, anchorAngle, defaultAim, color) {
  const bx = planet.x + Math.cos(anchorAngle) * planet.r;
  const by = planet.y + Math.sin(anchorAngle) * planet.r;
  return {
    id, planet, color,
    x: bx, y: by,          // base position on the planet surface
    anchorAngle,
    aim: defaultAim,       // barrel angle (world radians)
    power: 0.5,            // 0..1
    score: 0,
    cooldown: 0,
    connected: false,
    flash: 0,              // brief hit animation timer
  };
}

/** @type {{x:number,y:number,vx:number,vy:number,owner:number,life:number,trail:{x:number,y:number}[]}[]} */
let projectiles = [];

// High-level state machine for the screen.
let phase = 'lobby'; // 'lobby' | 'countdown' | 'playing' | 'paused' | 'gameover'
let countdown = 0;
let bannerTimer = 0;

// ---- Gravity field shared by the live sim AND the aim-preview ghost. -------
function gravityAt(x, y) {
  let ax = 0, ay = 0;
  for (const p of planets) {
    const dx = p.x - x, dy = p.y - y;
    const r2 = dx * dx + dy * dy + SOFTENING * SOFTENING;
    const invR = 1 / Math.sqrt(r2);
    const a = p.gm / r2; // magnitude = GM / r²
    ax += a * dx * invR; // times unit direction
    ay += a * dy * invR;
  }
  return { ax, ay };
}

// =============================  INPUT  =====================================

/**
 * Handle one controller packet arriving over the P2P data channel.
 * Packet shapes (kept tiny for latency):
 *   { t:'aim',   a:<radians> }
 *   { t:'power', p:<0..1> }
 *   { t:'fire' }
 */
function handleControllerInput(playerId, raw) {
  let msg;
  try { msg = JSON.parse(raw); } catch { return; }
  const turret = turrets[playerId - 1];
  if (!turret) return;

  switch (msg.t) {
    case 'aim':
      if (typeof msg.a === 'number') turret.aim = msg.a;
      break;
    case 'power':
      if (typeof msg.p === 'number') turret.power = Math.max(0, Math.min(1, msg.p));
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
  projectiles.push({
    x: sx, y: sy,
    vx: Math.cos(dir) * speed,
    vy: Math.sin(dir) * speed,
    owner: turret.id,
    life: PROJ_LIFETIME,
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
  // Timers (cooldowns, flashes, banners) advance in every phase.
  for (const t of turrets) {
    if (t.cooldown > 0) t.cooldown = Math.max(0, t.cooldown - dt);
    if (t.flash > 0) t.flash = Math.max(0, t.flash - dt);
  }
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

  // Integrate every projectile under the summed gravity of both planets.
  for (const p of projectiles) {
    const g = gravityAt(p.x, p.y);
    p.vx += g.ax * dt;
    p.vy += g.ay * dt;
    p.x += p.vx * dt;
    p.y += p.vy * dt;
    p.life -= dt;
    p.trail.push({ x: p.x, y: p.y });
    if (p.trail.length > 40) p.trail.shift();
  }

  // Resolve collisions and keep survivors.
  const survivors = [];
  for (const p of projectiles) {
    const outcome = classifyProjectile(p);
    if (outcome === 'alive') { survivors.push(p); continue; }
    if (outcome.hitTurret) onTurretHit(outcome.hitTurret, p.owner);
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
  // Hit a planet surface?
  for (const pl of planets) {
    const dx = pl.x - p.x, dy = pl.y - p.y;
    if (dx * dx + dy * dy <= (pl.r + PROJ_R) * (pl.r + PROJ_R)) return { reason: 'planet' };
  }
  // Hit a turret? (Including your own — a boomerang shot can backfire.)
  for (const t of turrets) {
    if (t.id === p.owner) continue; // your shot can't score on your own turret while leaving the muzzle...
    const dx = t.x - p.x, dy = t.y - p.y;
    if (dx * dx + dy * dy <= (TURRET_HIT_R + PROJ_R) * (TURRET_HIT_R + PROJ_R)) {
      return { reason: 'turret', hitTurret: t };
    }
  }
  return 'alive';
}

function onTurretHit(targetTurret, ownerId) {
  const shooter = turrets[ownerId - 1];
  if (!shooter) return;
  shooter.score += 1;
  targetTurret.flash = 0.8;
  projectiles = []; // clear the field for a clean restart of the volley

  if (shooter.score >= WIN_SCORE) {
    phase = 'gameover';
    showBanner(`PLAYER ${shooter.id} WINS`, shooter.color, 5, () => resetMatch());
  } else {
    showBanner(`PLAYER ${shooter.id} SCORES`, shooter.color, 1.2, () => { if (phase === 'playing') {} });
    // brief pause then continue
    phase = 'countdown';
    countdown = 1.2;
  }
}

function resetMatch() {
  for (const t of turrets) { t.score = 0; t.cooldown = 0; t.flash = 0; }
  projectiles = [];
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

  // Planets with glow + radial shading.
  for (const p of planets) {
    ctx.save();
    ctx.shadowColor = p.glow;
    ctx.shadowBlur = 60;
    const g = ctx.createRadialGradient(p.x - p.r * 0.3, p.y - p.r * 0.3, p.r * 0.1, p.x, p.y, p.r);
    g.addColorStop(0, '#ffffff');
    g.addColorStop(0.25, p.color);
    g.addColorStop(1, p.glow);
    ctx.fillStyle = g;
    ctx.beginPath();
    ctx.arc(p.x, p.y, p.r, 0, Math.PI * 2);
    ctx.fill();
    ctx.restore();
  }

  // Aim-trajectory previews (only while playing) — a ghost shot per player.
  if (phase === 'playing' || phase === 'countdown') {
    for (const t of turrets) if (t.connected) drawTrajectoryPreview(t);
  }

  // Turrets.
  for (const t of turrets) drawTurret(t);

  // Projectiles with trails.
  for (const p of projectiles) drawProjectile(p);

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
  // Trail.
  for (let i = 0; i < p.trail.length; i++) {
    const pt = p.trail[i];
    const a = i / p.trail.length;
    ctx.globalAlpha = a * 0.5;
    ctx.fillStyle = p.owner === 1 ? '#ff6b6b' : '#4dd0ff';
    ctx.beginPath();
    ctx.arc(pt.x, pt.y, PROJ_R * a, 0, Math.PI * 2);
    ctx.fill();
  }
  ctx.globalAlpha = 1;
  // Head.
  ctx.save();
  ctx.shadowColor = p.owner === 1 ? '#ff6b6b' : '#4dd0ff';
  ctx.shadowBlur = 18;
  ctx.fillStyle = '#fff';
  ctx.beginPath();
  ctx.arc(p.x, p.y, PROJ_R, 0, Math.PI * 2);
  ctx.fill();
  ctx.restore();
}

/** Simulate a lightweight ghost shot to show where the current aim/power lands. */
function drawTrajectoryPreview(t) {
  const dir = t.aim;
  const speed = MIN_SPEED + t.power * (MAX_SPEED - MIN_SPEED);
  let x = t.x + Math.cos(dir) * BARREL_LEN;
  let y = t.y + Math.sin(dir) * BARREL_LEN;
  let vx = Math.cos(dir) * speed;
  let vy = Math.sin(dir) * speed;
  const dt = 1 / 60;
  ctx.fillStyle = t.color;
  for (let i = 0; i < 90; i++) {
    const g = gravityAt(x, y);
    vx += g.ax * dt; vy += g.ay * dt;
    x += vx * dt; y += vy * dt;
    // Stop the preview at the first obstacle.
    let blocked = false;
    for (const pl of planets) {
      const dx = pl.x - x, dy = pl.y - y;
      if (dx * dx + dy * dy <= pl.r * pl.r) { blocked = true; break; }
    }
    if (blocked || x < -200 || x > WORLD_W + 200 || y < -200 || y > WORLD_H + 200) break;
    if (i % 3 === 0) {
      ctx.globalAlpha = 0.5 * (1 - i / 90);
      ctx.beginPath();
      ctx.arc(x, y, 3, 0, Math.PI * 2);
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

/** Reflect join/ready state in the lobby player slots. */
function updateLobby() {
  for (let id = 1; id <= 2; id++) {
    const slot = document.getElementById('slot-' + id);
    if (!slot) continue;
    const peer = peers.get(id);
    let state = 'empty', status = 'waiting…';
    if (peer && peer.ready) { state = 'ready'; status = 'ready ✔'; }
    else if (peer) { state = 'joined'; status = 'connecting…'; }
    slot.dataset.state = state;
    slot.querySelector('.status').textContent = status;
  }
}

/** Begin the match once BOTH players' data channels are live. */
function maybeStartMatch() {
  if (phase !== 'lobby' && phase !== 'paused') return;
  const p1 = peers.get(1), p2 = peers.get(2);
  if (p1 && p1.ready && p2 && p2.ready) {
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
connectSignaling();
requestAnimationFrame(frame);
requestAnimationFrame(syncScores);
