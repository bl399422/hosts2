// Holds the RTCPeerConnections (the MV3 service worker has no WebRTC).
// Dumb transport only: background.js owns all logic and signaling via the root server.
const CONFIG = { iceServers: [{ urls: "stun:stun.l.google.com:19302" }] };
const CONNECT_TIMEOUT_MS = 30000;

const conns = new Map(); // peerId -> { pc, dc, pending: [candidates] }
let myId = null;

const emit = (m) => chrome.runtime.sendMessage({ target: "background", ...m }).catch(() => {});

function drop(peerId, pc) {
  const c = conns.get(peerId);
  if (c && c.pc === pc) {
    conns.delete(peerId);
    emit({ event: "close", peerId });
  }
  try { pc.close(); } catch {}
}

function bindChannel(peerId, pc, dc) {
  const c = conns.get(peerId);
  if (c) c.dc = dc;
  dc.onopen = () => emit({ event: "open", peerId });
  dc.onmessage = (e) => emit({ event: "message", peerId, data: e.data });
  dc.onclose = () => drop(peerId, pc);
}

function create(peerId) {
  const pc = new RTCPeerConnection(CONFIG);
  const c = { pc, dc: null, pending: [] };
  conns.set(peerId, c);
  pc.onicecandidate = (e) =>
    e.candidate && emit({ event: "signal", peerId, data: { candidate: e.candidate.toJSON() } });
  pc.onconnectionstatechange = () => {
    if (["failed", "closed", "disconnected"].includes(pc.connectionState)) drop(peerId, pc);
  };
  setTimeout(() => {
    if (!c.dc || c.dc.readyState !== "open") drop(peerId, pc);
  }, CONNECT_TIMEOUT_MS);
  return c;
}

async function connect(peerId) {
  if (conns.has(peerId)) return;
  const c = create(peerId);
  bindChannel(peerId, c.pc, c.pc.createDataChannel("hosts2"));
  await c.pc.setLocalDescription();
  emit({ event: "signal", peerId, data: { description: c.pc.localDescription.toJSON() } });
}

async function flush(c) {
  for (const cand of c.pending.splice(0)) await c.pc.addIceCandidate(cand).catch(() => {});
}

async function onSignal(peerId, data) {
  if (!data || typeof data !== "object") return;
  let c = conns.get(peerId);

  if (data.description?.type === "offer") {
    if (c) {
      // Glare: both sides offered; the lower id's offer wins.
      if (c.dc?.readyState !== "open" && myId && myId < peerId) return;
      drop(peerId, c.pc);
    }
    c = create(peerId);
    c.pc.ondatachannel = (e) => bindChannel(peerId, c.pc, e.channel);
    await c.pc.setRemoteDescription(data.description);
    await flush(c);
    await c.pc.setLocalDescription();
    emit({ event: "signal", peerId, data: { description: c.pc.localDescription.toJSON() } });
  } else if (data.description?.type === "answer") {
    if (c && c.pc.signalingState === "have-local-offer") {
      await c.pc.setRemoteDescription(data.description);
      await flush(c);
    }
  } else if (data.candidate && c) {
    if (c.pc.remoteDescription) await c.pc.addIceCandidate(data.candidate).catch(() => {});
    else c.pending.push(data.candidate);
  }
}

chrome.runtime.onMessage.addListener((msg, sender, sendResponse) => {
  if (msg?.target !== "offscreen" || sender.id !== chrome.runtime.id) return;
  switch (msg.cmd) {
    case "setId": myId = msg.id; break;
    case "connect": connect(msg.peerId).catch(() => conns.has(msg.peerId) && drop(msg.peerId, conns.get(msg.peerId).pc)); break;
    case "signal": onSignal(msg.peerId, msg.data).catch(() => {}); break;
    case "send": {
      const dc = conns.get(msg.peerId)?.dc;
      if (dc?.readyState === "open") dc.send(msg.data);
      break;
    }
    case "close": {
      const c = conns.get(msg.peerId);
      if (c) drop(msg.peerId, c.pc);
      break;
    }
    case "closeAll":
      for (const [id, c] of [...conns]) drop(id, c.pc);
      break;
    case "list":
      sendResponse([...conns].filter(([, c]) => c.dc?.readyState === "open").map(([id]) => id));
      break;
  }
});
