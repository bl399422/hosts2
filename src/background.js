// Logs domain -> IP resolutions (first time each pair is seen) to storage.
// Rows are exported as hosts2.csv from the popup.
const KEY = "records"; // { "domain,ip": timestamp }

async function getState() {
  const s = await chrome.storage.local.get({ enabled: false });
  return s.enabled;
}

// Serialize writes so concurrent requests don't clobber each other.
let queue = Promise.resolve();
function record(domain, ip) {
  queue = queue.then(async () => {
    const id = `${domain},${ip}`;
    const data = (await chrome.storage.local.get({ [KEY]: {} }))[KEY];
    if (data[id]) return;
    data[id] = new Date().toISOString();
    await chrome.storage.local.set({ [KEY]: data });
    broadcastDelta();
  }).catch(console.error);
}

function clearData(scope) {
  const keys = scope === "hosts" ? [KEY]
    : scope === "peers" ? ["peers"]
      : scope === "all" ? [KEY, "peers"]
        : null;
  if (!keys) return Promise.reject(new Error("Invalid data scope."));

  const operation = queue.then(async () => {
    await chrome.storage.local.remove(keys);
    for (const c of conns.values()) {
      if (keys.includes(KEY)) c.sentHosts.clear();
      if (keys.includes("peers")) c.sentPeers.clear();
    }
  });
  queue = operation.catch(console.error);
  return operation;
}

chrome.webRequest.onCompleted.addListener(
  async (details) => {
    if (!details.ip || details.fromCache) return;
    if (!(await getState())) return;
    try {
      const host = new URL(details.url).hostname;
      record(host, details.ip.replace(/^\[|\]$/g, ""));
    } catch (e) {}
  },
  { urls: ["<all_urls>"] }
);

chrome.runtime.onStartup.addListener(async () => {
  const s = await chrome.storage.local.get({ autoStart: false });
  if (s.autoStart) await chrome.storage.local.set({ enabled: true });
});

// ---- Peers ----
// Storage "peers": { [id]: { id, ip, source: "root"|"peer", preferred, firstSeen, lastSeen } }
// Root-announced rows are mirrored in place; rows learned from peers or marked preferred are kept.
const SERVER_URL = "ws://localhost:8080";
const PREFERRED_TARGET = 10;
const MAX_CONNECTIONS = 30;
const RETRY_MS = 2 * 60 * 1000;
const BATCH = 300;
const MAX_MSG = 1024 * 1024;

let ws;
let myId = null;
const conns = new Map(); // peerId -> { sentHosts: Map, sentPeers: Set, sharePeers, asked, ok, bad, chain }
const attempts = new Map(); // peerId -> last connect attempt time

const getPeers = async () => (await chrome.storage.local.get({ peers: {} })).peers;
const getRecords = async () => (await chrome.storage.local.get({ [KEY]: {} }))[KEY];

// ---- Verification ----
// Chrome extensions cannot inspect certificates or pin an IP, so a claim (domain, ip) is accepted when
// an independent DoH lookup resolves the domain to that ip and the domain is reachable over valid HTTPS.
const DOMAIN_RE = /^(?=.{1,253}$)([a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z][a-z0-9-]{1,62}$/i;
const verifyCache = new Map();
const VERIFY_TTL = 10 * 60 * 1000;

function canonicalIp(ip) {
  if (typeof ip !== "string") return null;
  const v4 = /^\d{1,3}(\.\d{1,3}){3}$/.test(ip);
  if (!v4 && !(ip.includes(":") && /^[0-9a-f:.]+$/i.test(ip))) return null;
  try {
    return new URL(`http://${v4 ? ip : `[${ip}]`}`).hostname.replace(/^\[|\]$/g, "");
  } catch {
    return null;
  }
}

async function resolvesTo(domain, ip) {
  for (const type of ["A", "AAAA"]) {
    try {
      const r = await fetch(
        `https://cloudflare-dns.com/dns-query?name=${encodeURIComponent(domain)}&type=${type}`,
        { headers: { accept: "application/dns-json" }, signal: AbortSignal.timeout(8000) }
      );
      const j = await r.json();
      if ((j.Answer || []).some((a) => (a.type === 1 || a.type === 28) && canonicalIp(a.data) === ip)) return true;
    } catch {}
  }
  return false;
}

async function isOnline(domain) {
  try {
    await fetch(`https://${domain}/`, {
      method: "HEAD", mode: "no-cors", cache: "no-store", credentials: "omit",
      signal: AbortSignal.timeout(8000),
    });
    return true;
  } catch {
    return false;
  }
}

let active = 0;
const waiting = [];
async function limited(fn) {
  if (active >= 5) await new Promise((r) => waiting.push(r));
  active++;
  try {
    return await fn();
  } finally {
    active--;
    waiting.shift()?.();
  }
}

function verifyEntry(domain, rawIp) {
  const ip = canonicalIp(rawIp);
  if (typeof domain !== "string" || !DOMAIN_RE.test(domain) || !ip) return Promise.resolve(false);
  const key = `${domain.toLowerCase()},${ip}`;
  const hit = verifyCache.get(key);
  if (hit && Date.now() - hit.at < VERIFY_TTL) return hit.result;
  const result = limited(async () => (await resolvesTo(domain, ip)) && (await isOnline(domain)));
  verifyCache.set(key, { at: Date.now(), result });
  return result;
}

// ---- Offscreen (WebRTC) ----
let offscreenReady;
function ensureOffscreen() {
  offscreenReady ||= (async () => {
    const ctx = await chrome.runtime.getContexts({ contextTypes: ["OFFSCREEN_DOCUMENT"] });
    if (!ctx.length) {
      await chrome.offscreen.createDocument({
        url: "offscreen.html",
        reasons: ["WEB_RTC"],
        justification: "Peer-to-peer Hosts2 list exchange",
      });
    }
  })().catch((e) => { offscreenReady = null; throw e; });
  return offscreenReady;
}

async function rtc(cmd, extra = {}) {
  await ensureOffscreen();
  return chrome.runtime.sendMessage({ target: "offscreen", cmd, ...extra });
}

const sendTo = (peerId, obj) => rtc("send", { peerId, data: JSON.stringify(obj) }).catch(() => {});

// ---- Sending (deltas only: what a connection has already seen is never re-sent) ----
async function sendHosts(peerId, hello) {
  const c = conns.get(peerId);
  if (!c) return;
  const records = await getRecords();
  const fresh = Object.entries(records).filter(([id, ts]) => c.sentHosts.get(id) !== ts);
  if (!fresh.length && !hello) return;
  for (let i = 0; i === 0 || i < fresh.length; i += BATCH) {
    const slice = fresh.slice(i, i + BATCH);
    for (const [id, ts] of slice) c.sentHosts.set(id, ts);
    const hosts = slice.map(([id, ts]) => {
      const k = id.lastIndexOf(",");
      return [id.slice(0, k), id.slice(k + 1), ts];
    });
    sendTo(peerId, { type: "hosts", hosts, hello, last: i + BATCH >= fresh.length });
  }
}

async function sendPeerList(peerId) {
  const c = conns.get(peerId);
  if (!c?.sharePeers) return;
  const rows = Object.values(await getPeers()).filter(
    (p) => p.id !== peerId && p.id !== myId && p.ip && !c.sentPeers.has(p.id)
  );
  for (let i = 0; i < rows.length; i += BATCH) {
    const slice = rows.slice(i, i + BATCH);
    slice.forEach((p) => c.sentPeers.add(p.id));
    sendTo(peerId, { type: "peers", peers: slice.map((p) => ({ id: p.id, ip: p.ip })) });
  }
}

function broadcastDelta() {
  for (const peerId of conns.keys()) {
    sendHosts(peerId, false).catch(console.error);
    sendPeerList(peerId).catch(console.error);
  }
}

// ---- Receiving ----
function modifyPeers(fn) {
  queue = queue.then(async () => {
    const peers = await getPeers();
    if (fn(peers) !== false) await chrome.storage.local.set({ peers });
  }).catch(console.error);
  return queue;
}

async function processHosts(peerId, msg) {
  const c = conns.get(peerId);
  if (!c || !Array.isArray(msg.hosts)) return;
  const records = await getRecords();
  const limit = Date.now() + 24 * 3600 * 1000;
  const accepted = [];

  await Promise.all(msg.hosts.slice(0, BATCH * 2).map(async (e) => {
    const [domain, rawIp, ts] = Array.isArray(e) ? e : [];
    const ip = canonicalIp(rawIp);
    const time = Date.parse(ts);
    if (typeof domain !== "string" || !ip || !(time <= limit)) return void c.bad++;
    const id = `${domain.toLowerCase()},${ip}`;
    c.sentHosts.set(id, ts); // never echo it back to its source
    if (records[id] && records[id] >= ts) return void c.ok++;
    if (await verifyEntry(domain, ip)) {
      accepted.push([id, ts]);
      c.ok++;
    } else {
      c.bad++;
    }
  }));

  if (accepted.length) {
    queue = queue.then(async () => {
      const data = await getRecords();
      for (const [id, ts] of accepted) if (!data[id] || data[id] < ts) data[id] = ts;
      await chrome.storage.local.set({ [KEY]: data });
    }).catch(console.error);
    await queue;
    broadcastDelta();
  }

  // Preferred only when the peer's entire hello list verified; any later bad entry revokes it.
  const preferred = c.bad === 0 && c.ok > 0 && (msg.hello && msg.last ? true : null);
  if (c.bad > 0 || preferred) {
    await modifyPeers((peers) => {
      const row = peers[peerId];
      if (!row || row.preferred === (c.bad === 0)) return false;
      row.preferred = c.bad === 0;
    });
    evaluate();
  }
}

async function processPeerList(peerId, msg) {
  const c = conns.get(peerId);
  if (!c || !Array.isArray(msg.peers)) return;
  const now = new Date().toISOString();
  const incoming = msg.peers
    .slice(0, BATCH)
    .filter((p) => p && typeof p.id === "string" && p.id.length <= 64 && canonicalIp(p.ip));
  incoming.forEach((p) => c.sentPeers.add(p.id));
  await modifyPeers((peers) => {
    let changed = false;
    for (const p of incoming) {
      if (p.id === myId || peers[p.id]) continue;
      // Peer-supplied rows are never trusted as preferred.
      peers[p.id] = { id: p.id, ip: canonicalIp(p.ip), source: "peer", preferred: false, firstSeen: now, lastSeen: now };
      changed = true;
    }
    return changed;
  });
  broadcastDelta();
  evaluate();
}

function syncPeers(list) {
  return modifyPeers((old) => {
    const now = new Date().toISOString();
    const next = {};
    for (const [id, row] of Object.entries(old)) {
      if (row.source === "peer" || row.preferred) next[id] = row;
    }
    for (const p of list) {
      const o = old[p.id];
      next[p.id] = {
        id: p.id, ip: p.ip, source: "root", preferred: o?.preferred || false,
        firstSeen: o?.firstSeen || now, lastSeen: now,
      };
    }
    for (const k of Object.keys(old)) delete old[k];
    Object.assign(old, next);
  }).then(() => { broadcastDelta(); evaluate(); });
}

// ---- Connection management ----
async function preferredOnline() {
  const peers = await getPeers();
  return [...conns.keys()].filter((id) => peers[id]?.preferred).length;
}

// With enough preferred peers online: stop using the root server and ask peers for their peer lists.
// Otherwise: stay on the root server and connect to known peers until the target is reached.
let evaluating = false;
async function evaluate() {
  if (evaluating) return;
  evaluating = true;
  try {
    if (!(await getState())) return;
    const online = await preferredOnline();
    if (online >= PREFERRED_TARGET) {
      if (ws) ws.close();
      for (const [peerId, c] of conns) {
        if (!c.asked) {
          c.asked = true;
          sendTo(peerId, { type: "getPeers" });
        }
      }
      return;
    }
    connectRoot();
    const peers = await getPeers();
    const now = Date.now();
    for (const p of Object.values(peers)) {
      if (conns.size >= MAX_CONNECTIONS) break;
      if (p.id === myId || conns.has(p.id) || now - (attempts.get(p.id) || 0) < RETRY_MS) continue;
      if (ws?.readyState !== WebSocket.OPEN) break;
      attempts.set(p.id, now);
      rtc("connect", { peerId: p.id }).catch(() => {});
    }
  } finally {
    evaluating = false;
  }
}

function connectRoot() {
  if (ws && (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING)) return;
  ws = new WebSocket(SERVER_URL);
  ws.onmessage = (e) => {
    try {
      const msg = JSON.parse(e.data);
      if (msg.type === "welcome" && typeof msg.id === "string") {
        myId = msg.id;
        rtc("setId", { id: myId }).catch(() => {});
      } else if (msg.type === "peers" && Array.isArray(msg.peers)) {
        syncPeers(msg.peers.filter((p) => typeof p?.id === "string" && canonicalIp(p.ip)));
      } else if (msg.type === "signal" && typeof msg.from === "string") {
        rtc("signal", { peerId: msg.from, data: msg.data }).catch(() => {});
      }
    } catch {}
  };
  ws.onclose = () => evaluate();
  ws.onerror = () => {};
}

function onOpen(peerId) {
  conns.set(peerId, { sentHosts: new Map(), sentPeers: new Set(), sharePeers: false, asked: false, ok: 0, bad: 0, chain: Promise.resolve() });
  sendHosts(peerId, true).catch(console.error);
}

chrome.runtime.onMessage.addListener((msg, sender, sendResponse) => {
  if (msg?.target !== "background" || sender.id !== chrome.runtime.id) return;
  if (msg.cmd === "clearData") {
    clearData(msg.scope).then(
      () => sendResponse({ ok: true }),
      (error) => sendResponse({ ok: false, error: error.message })
    );
    return true;
  }
  const { event, peerId } = msg;
  if (typeof peerId !== "string") return;
  if (event === "signal") {
    if (ws?.readyState === WebSocket.OPEN) ws.send(JSON.stringify({ type: "signal", to: peerId, data: msg.data }));
  } else if (event === "open") {
    onOpen(peerId);
    evaluate();
  } else if (event === "close") {
    conns.delete(peerId);
    evaluate();
  } else if (event === "message") {
    const c = conns.get(peerId);
    if (!c || typeof msg.data !== "string" || msg.data.length > MAX_MSG) return;
    let m;
    try { m = JSON.parse(msg.data); } catch { return; }
    // Serialized per peer so batches are verified in order.
    c.chain = c.chain.then(async () => {
      if (m.type === "hosts") await processHosts(peerId, m);
      else if (m.type === "getPeers") { c.sharePeers = true; await sendPeerList(peerId); }
      else if (m.type === "peers") await processPeerList(peerId, m);
    }).catch(console.error);
  }
});

async function stop() {
  if (ws) {
    ws.onclose = null;
    ws.close();
    ws = null;
  }
  conns.clear();
  attempts.clear();
  const ctx = await chrome.runtime.getContexts({ contextTypes: ["OFFSCREEN_DOCUMENT"] });
  if (ctx.length) {
    await chrome.runtime.sendMessage({ target: "offscreen", cmd: "closeAll" }).catch(() => {});
    await chrome.offscreen.closeDocument().catch(() => {});
  }
  offscreenReady = null;
}

async function start() {
  if (!(await getState())) return stop();
  await ensureOffscreen();
  // After a service worker restart the offscreen document may still hold open channels: resume them.
  for (const peerId of (await rtc("list")) || []) if (!conns.has(peerId)) onOpen(peerId);
  evaluate();
}

// The alarm wakes the service worker and reconnects if the socket dropped.
chrome.storage.onChanged.addListener((changes, area) => {
  if (area === "local" && changes.enabled) start().catch(console.error);
});

chrome.alarms.create("reconnect", { periodInMinutes: 0.5 });
chrome.alarms.onAlarm.addListener((a) => a.name === "reconnect" && start().catch(console.error));
chrome.runtime.onStartup.addListener(() => start().catch(console.error));
chrome.runtime.onInstalled.addListener(() => start().catch(console.error));
start().catch(console.error);
