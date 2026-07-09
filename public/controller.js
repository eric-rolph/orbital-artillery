/* ==========================================================================
 * Orbital Artillery — SMARTPHONE CONTROLLER
 * ==========================================================================
 *
 * The phone is a dumb, low-latency gamepad. Flow:
 *   1. Open a WebSocket to the signaling DO:  /join?code=ABCD
 *   2. Receive a player id, then act as the WebRTC ANSWERER: the Screen sends us
 *      an SDP offer (it created the DataChannel), we answer, we trickle ICE.
 *   3. The moment the DataChannel opens we stop touching the WebSocket for
 *      gameplay and stream touch input straight to the Screen over P2P WebRTC.
 *
 * We send only three tiny message shapes, coalesced to one send per animation
 * frame so a fast-moving thumb can't flood the channel:
 *      { t:'aim',   a:<radians> }   – barrel angle
 *      { t:'power', p:<0..1> }      – muzzle power
 *      { t:'fire' }                 – shoot (sent immediately, not coalesced)
 * ========================================================================== */

'use strict';

const RTC_CONFIG = { iceServers: [{ urls: 'stun:stun.l.google.com:19302' }] };
const PLAYER_COLORS = { 1: '#ff6b6b', 2: '#4dd0ff' };

// ------- Connection state -------
let ws = null;
let pc = null;
let channel = null;
let playerId = null;
let roomCode = '';
let pendingIce = []; // ICE candidates that arrive before our remote description is set

// ------- Input state (streamed to the Screen) -------
let aim = 0;          // radians
let power = 0.5;      // 0..1
let aimDirty = false;
let powerDirty = false;

// ==========================  DOM  ==========================
const gate = document.getElementById('gate');
const gateMsg = document.getElementById('gate-msg');
const codeInput = document.getElementById('code-input');
const connectBtn = document.getElementById('connect-btn');
const whoLabel = document.getElementById('who-label');
const roomLabel = document.getElementById('room-label');
const badge = document.getElementById('badge');
const pad = document.getElementById('pad');
const knob = document.getElementById('knob');
const barrelHint = document.getElementById('barrel-hint');
const powerTrack = document.getElementById('power-track');
const powerFill = document.getElementById('power-fill');
const powerThumb = document.getElementById('power-thumb');
const powerPct = document.getElementById('power-pct');
const fireBtn = document.getElementById('fire');

// ==========================  BOOT  ==========================
// If the deep link carried ?code=ABCD (e.g. from the QR code), auto-connect.
(function init() {
  const url = new URL(location.href);
  const code = (url.searchParams.get('code') || '').toUpperCase();
  if (/^[A-Z2-9]{4}$/.test(code)) {
    codeInput.value = code;
    connect(code);
  }
  codeInput.addEventListener('input', () => {
    codeInput.value = codeInput.value.toUpperCase().replace(/[^A-Z2-9]/g, '').slice(0, 4);
  });
  connectBtn.addEventListener('click', () => {
    const c = codeInput.value.trim().toUpperCase();
    if (/^[A-Z2-9]{4}$/.test(c)) connect(c);
    else gateMsg.textContent = 'Enter a valid 4-character code.';
  });
  codeInput.addEventListener('keydown', (e) => { if (e.key === 'Enter') connectBtn.click(); });
})();

// ==========================  SIGNALING  ==========================
function connect(code) {
  roomCode = code;
  gateMsg.textContent = '';
  gateMsg.innerHTML = '<div class="spinner"></div>';

  const proto = location.protocol === 'https:' ? 'wss' : 'ws';
  ws = new WebSocket(`${proto}://${location.host}/api/join?code=${code}`);

  ws.addEventListener('message', async (ev) => {
    const msg = JSON.parse(ev.data);
    switch (msg.type) {
      case 'joined':
        playerId = msg.playerId;
        onJoined();
        break;
      case 'signal':
        await handleSignal(msg.data);
        break;
      case 'error':
        showGateError(msg.message || 'Could not join room.');
        break;
      case 'screen-disconnected':
        showGateError('The screen went offline. Ask the host to reopen the game.');
        setDisconnected();
        break;
    }
  });

  ws.addEventListener('error', () => showGateError('Connection error. Check the code and try again.'));
  ws.addEventListener('close', () => {
    // If we never made it into a room, surface it on the gate.
    if (!playerId) showGateError('Could not reach the room. Check the code.');
  });
}

function showGateError(text) {
  gate.classList.remove('hidden');
  gateMsg.textContent = text;
}

/** We have a player id — build the (answerer) peer connection and reveal the UI. */
function onJoined() {
  const color = PLAYER_COLORS[playerId] || '#7cc4ff';
  document.documentElement.style.setProperty('--p', color);
  whoLabel.textContent = `PLAYER ${playerId}`;
  roomLabel.textContent = `ROOM ${roomCode}`;
  badge.style.background = color;

  createPeer();
}

/**
 * Create our RTCPeerConnection as the ANSWERER. We do NOT create a data channel;
 * the Screen (offerer) already put one in its offer, so we receive it via
 * `ondatachannel`. We create the PC eagerly so it exists before the offer lands.
 */
function createPeer() {
  pc = new RTCPeerConnection(RTC_CONFIG);

  // Trickle our ICE candidates back to the Screen (DO infers the target = screen).
  pc.addEventListener('icecandidate', (e) => {
    if (e.candidate) wsSignal({ candidate: e.candidate });
  });

  // The Screen's data channel arrives here.
  pc.addEventListener('datachannel', (e) => {
    channel = e.channel;
    channel.addEventListener('open', onChannelOpen);
    channel.addEventListener('close', onChannelClose);
  });

  pc.addEventListener('connectionstatechange', () => {
    if (['failed', 'disconnected', 'closed'].includes(pc.connectionState)) setDisconnected();
  });
}

/** Handle an inbound SDP offer or ICE candidate from the Screen. */
async function handleSignal(data) {
  if (!pc) createPeer();

  if (data.sdp) {
    // The Screen's OFFER. Answer it.
    await pc.setRemoteDescription(new RTCSessionDescription(data.sdp));
    const answer = await pc.createAnswer();
    await pc.setLocalDescription(answer);
    wsSignal({ sdp: pc.localDescription });
    // Flush any ICE candidates that beat the offer here.
    for (const c of pendingIce.splice(0)) {
      try { await pc.addIceCandidate(new RTCIceCandidate(c)); } catch (err) { console.warn('ICE add failed', err); }
    }
  } else if (data.candidate) {
    if (pc.remoteDescription && pc.remoteDescription.type) {
      try { await pc.addIceCandidate(new RTCIceCandidate(data.candidate)); } catch (err) { console.warn('ICE add failed', err); }
    } else {
      pendingIce.push(data.candidate);
    }
  }
}

/** Send a signaling payload up to the Screen through the DO (only during handshake). */
function wsSignal(data) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify({ type: 'signal', data }));
  }
}

// ==========================  CHANNEL LIFECYCLE  ==========================
function onChannelOpen() {
  gate.classList.add('hidden');
  document.body.classList.remove('disconnected');
  fireBtn.disabled = false;
  whoLabel.textContent = `PLAYER ${playerId}`;
  // Push the initial aim/power so the Screen matches the dial on first paint.
  aimDirty = powerDirty = true;
  if (navigator.vibrate) navigator.vibrate(30);
}

function onChannelClose() { setDisconnected(); }

function setDisconnected() {
  document.body.classList.add('disconnected');
  fireBtn.disabled = true;
  whoLabel.textContent = 'DISCONNECTED';
}

/** Reliable send helper; drops silently if the channel isn't open yet. */
function send(obj) {
  if (channel && channel.readyState === 'open') {
    try { channel.send(JSON.stringify(obj)); } catch {}
  }
}

// ==========================  INPUT: AIM DIAL  ==========================
// The pad is a rotary dial. Touching/dragging sets the barrel ANGLE (magnitude is
// ignored). The knob and a barrel hint always sit on the rim in the aim direction.
let padActive = false;

function padAngleFromEvent(clientX, clientY) {
  const rect = pad.getBoundingClientRect();
  const cx = rect.left + rect.width / 2;
  const cy = rect.top + rect.height / 2;
  return Math.atan2(clientY - cy, clientX - cx);
}

function setAim(a) {
  aim = a;
  aimDirty = true;
  // Visual: place the knob on the rim (radius ~ 37% of pad) and rotate the hint.
  const rimPct = 37;
  const kx = 50 + Math.cos(a) * rimPct;
  const ky = 50 + Math.sin(a) * rimPct;
  knob.style.left = kx + '%';
  knob.style.top = ky + '%';
  barrelHint.style.transform = `rotate(${a}rad)`;
}

function onPadStart(e) {
  padActive = true;
  const t = e.touches ? e.touches[0] : e;
  setAim(padAngleFromEvent(t.clientX, t.clientY));
  e.preventDefault();
}
function onPadMove(e) {
  if (!padActive) return;
  const t = e.touches ? e.touches[0] : e;
  setAim(padAngleFromEvent(t.clientX, t.clientY));
  e.preventDefault();
}
function onPadEnd() { padActive = false; }

pad.addEventListener('touchstart', onPadStart, { passive: false });
pad.addEventListener('touchmove', onPadMove, { passive: false });
pad.addEventListener('touchend', onPadEnd);
pad.addEventListener('touchcancel', onPadEnd);
// Mouse fallback so the controller is testable on a laptop.
pad.addEventListener('mousedown', onPadStart);
window.addEventListener('mousemove', onPadMove);
window.addEventListener('mouseup', onPadEnd);

// ==========================  INPUT: POWER SLIDER  ==========================
let powerActive = false;

function setPowerFromEvent(clientX) {
  const rect = powerTrack.getBoundingClientRect();
  let v = (clientX - rect.left) / rect.width;
  v = Math.max(0, Math.min(1, v));
  power = v;
  powerDirty = true;
  powerFill.style.width = v * 100 + '%';
  powerThumb.style.left = v * 100 + '%';
  powerPct.textContent = Math.round(v * 100) + '%';
}

function onPowerStart(e) {
  powerActive = true;
  const t = e.touches ? e.touches[0] : e;
  setPowerFromEvent(t.clientX);
  e.preventDefault();
}
function onPowerMove(e) {
  if (!powerActive) return;
  const t = e.touches ? e.touches[0] : e;
  setPowerFromEvent(t.clientX);
  e.preventDefault();
}
function onPowerEnd() { powerActive = false; }

powerTrack.addEventListener('touchstart', onPowerStart, { passive: false });
powerTrack.addEventListener('touchmove', onPowerMove, { passive: false });
powerTrack.addEventListener('touchend', onPowerEnd);
powerTrack.addEventListener('touchcancel', onPowerEnd);
powerTrack.addEventListener('mousedown', onPowerStart);
window.addEventListener('mousemove', onPowerMove);
window.addEventListener('mouseup', onPowerEnd);

// Initialize the slider visuals to the default power.
setPowerFromEvent(powerTrack.getBoundingClientRect().left + powerTrack.getBoundingClientRect().width * power);

// ==========================  INPUT: FIRE  ==========================
function doFire(e) {
  if (e) e.preventDefault();
  if (!channel || channel.readyState !== 'open') return;
  send({ t: 'fire' });
  if (navigator.vibrate) navigator.vibrate(45); // punchy haptic
}
fireBtn.addEventListener('touchstart', doFire, { passive: false });
fireBtn.addEventListener('mousedown', doFire);

// ==========================  SEND LOOP  ==========================
// Coalesce aim/power to at most one message per frame — smooth for the Screen,
// gentle on the channel. Fire is sent immediately (above), never coalesced.
function pump() {
  requestAnimationFrame(pump);
  if (!channel || channel.readyState !== 'open') return;
  if (aimDirty) { send({ t: 'aim', a: aim }); aimDirty = false; }
  if (powerDirty) { send({ t: 'power', p: power }); powerDirty = false; }
}
requestAnimationFrame(pump);

// ==========================  ZOOM / SCROLL LOCK  ==========================
// Belt-and-suspenders on top of the viewport meta + CSS touch-action:none.
document.addEventListener('gesturestart', (e) => e.preventDefault()); // iOS pinch
document.addEventListener('dblclick', (e) => e.preventDefault());     // double-tap zoom
document.addEventListener('touchmove', (e) => {
  // Allow scrolling only inside a genuinely scrollable element (none here).
  if (e.cancelable) e.preventDefault();
}, { passive: false });
