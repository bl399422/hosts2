// Test swarm: spawns dummy Hosts2 peers that join the root server, open WebRTC data channels to
// other peers (dummy or real extensions), exchange hosts/peer lists and "visit" random sites.
// Everything lives in memory and is discarded on exit. A live dashboard is served over HTTP.
//
// Usage: npm run swarm -- --peers 5 ...  or  node test/swarm.js [--peers 5] [--server ws://localhost:8080] [--port 8081] [--visit 4000]
const http = require('http');
const fs = require('fs');
const path = require('path');
const dns = require('dns').promises;
const { WebSocket } = require('ws');
const nodeDataChannel = require('node-datachannel');

const arg = (name, def) => {
  const i = process.argv.indexOf(`--${name}`);
  if (i > -1 && process.argv[i + 1]) return process.argv[i + 1];
  // `npm run swarm --peers 25` (no `--`) is consumed by npm and exposed as an env var.
  return process.env[`npm_config_${name}`] || process.env[name.toUpperCase()] || def;
};
const PEERS = Number(arg('peers', 5));
const SERVER_URL = arg('server', 'ws://localhost:8080');
const DASH_PORT = Number(arg('port', 8081));
const VISIT_MS = Number(arg('visit', 4000));
const RETRY_MS = 30000;
const CONNECT_TIMEOUT_MS = 30000;
const MAX_MSG = 1024 * 1024;
const BATCH = 300;

const SITES = [
  'example.com', 'wikipedia.org', 'github.com', 'mozilla.org', 'archive.org', 'python.org',
  'nodejs.org', 'debian.org', 'kernel.org', 'gnu.org', 'apache.org', 'openstreetmap.org',
  'cloudflare.com', 'duckduckgo.com', 'stackoverflow.com', 'reddit.com', 'bbc.com', 'nasa.gov',
  'eff.org', 'rust-lang.org', 'golang.org', 'ubuntu.com', 'wordpress.org', 'npmjs.com',
];

// ---- Dashboard ----
const sse = new Set();
const dummies = [];
const known = new Map(); // id -> { id, dummy }

function emit(ev) {
  const line = `data: ${JSON.stringify({ t: Date.now(), ...ev })}\n\n`;
  for (const res of sse) res.write(line);
}

function snapshot() {
  const nodes = new Map();
  const links = new Set();
  for (const d of dummies) {
    if (!d.id) continue;
    nodes.set(d.id, { id: d.id, dummy: true });
    for (const [pid, c] of d.conns) {
      if (c.dc?.isOpen()) {
        if (!nodes.has(pid)) nodes.set(pid, { id: pid, dummy: known.get(pid)?.dummy || false });
        links.add([d.id, pid].sort().join('|'));
      }
    }
  }
  return { nodes: [...nodes.values()], links: [...links].map((l) => l.split('|')) };
}

const server = http.createServer((req, res) => {
  if (req.url === '/events') {
    res.writeHead(200, { 'content-type': 'text/event-stream', 'cache-control': 'no-cache', connection: 'keep-alive' });
    res.write(`data: ${JSON.stringify({ t: Date.now(), kind: 'snapshot', ...snapshot() })}\n\n`);
    sse.add(res);
    req.on('close', () => sse.delete(res));
  } else if (req.url === '/' || req.url === '/index.html') {
    res.writeHead(200, { 'content-type': 'text/html; charset=utf-8' });
    res.end(fs.readFileSync(path.join(__dirname, 'dashboard.html')));
  } else {
    res.writeHead(404).end();
  }
});

// ---- Dummy peer ----
class Dummy {
  constructor(n) {
    this.n = n;
    this.id = null;
    this.records = new Map(); // "domain,ip" -> iso timestamp
    this.peers = new Map(); // id -> ip learned from root or peers
    this.conns = new Map(); // peerId -> { pc, dc, pending, sentHosts, sentPeers, sharePeers }
    this.attempts = new Map();
    this.closed = false;
    this.connectRoot();
    this.visitTimer = setTimeout(() => this.visitLoop(), Math.random() * VISIT_MS);
    this.retryTimer = setInterval(() => this.connectAll(), 5000);
  }

  connectRoot() {
    if (this.closed) return;
    const ws = (this.ws = new WebSocket(SERVER_URL));
    ws.on('message', (raw) => {
      let msg;
      try { msg = JSON.parse(raw.toString()); } catch { return; }
      if (msg.type === 'welcome') {
        this.id = msg.id;
        known.set(this.id, { id: this.id, dummy: true });
        emit({ kind: 'join', id: this.id, dummy: true });
      } else if (msg.type === 'peers' && Array.isArray(msg.peers)) {
        for (const p of msg.peers) if (typeof p?.id === 'string') this.peers.set(p.id, p.ip);
        this.connectAll();
      } else if (msg.type === 'signal' && typeof msg.from === 'string') {
        this.onSignal(msg.from, msg.data);
      }
    });
    ws.on('close', () => {
      if (this.id) emit({ kind: 'leave', id: this.id });
      this.id = null;
      if (!this.closed) setTimeout(() => this.connectRoot(), 3000);
    });
    ws.on('error', () => {});
  }

  signal(peerId, data) {
    if (this.ws?.readyState === WebSocket.OPEN) this.ws.send(JSON.stringify({ type: 'signal', to: peerId, data }));
  }

  connectAll() {
    if (!this.id) return;
    const now = Date.now();
    for (const peerId of this.peers.keys()) {
      if (peerId === this.id || this.conns.has(peerId) || now - (this.attempts.get(peerId) || 0) < RETRY_MS) continue;
      this.attempts.set(peerId, now);
      this.connect(peerId);
    }
  }

  create(peerId) {
    const pc = new nodeDataChannel.PeerConnection(`dummy-${this.n}`, {
      iceServers: ['stun:stun.l.google.com:19302'],
    });
    const c = { pc, dc: null, pending: [], sentHosts: new Map(), sentPeers: new Set(), sharePeers: false };
    this.conns.set(peerId, c);
    pc.onLocalDescription((sdp, type) => this.signal(peerId, { description: { type, sdp } }));
    pc.onLocalCandidate((candidate, sdpMid) => this.signal(peerId, { candidate: { candidate, sdpMid, sdpMLineIndex: 0 } }));
    pc.onStateChange((state) => {
      if (['failed', 'closed', 'disconnected'].includes(state)) this.drop(peerId, pc);
    });
    setTimeout(() => {
      if (this.conns.get(peerId) === c && !c.dc?.isOpen()) this.drop(peerId, pc);
    }, CONNECT_TIMEOUT_MS).unref();
    return c;
  }

  connect(peerId) {
    const c = this.create(peerId);
    this.bind(peerId, c, c.pc.createDataChannel('hosts2'));
  }

  bind(peerId, c, dc) {
    c.dc = dc;
    dc.onOpen(() => {
      emit({ kind: 'connect', from: this.id, to: peerId, toDummy: known.get(peerId)?.dummy || false });
      this.sendHosts(peerId, true);
    });
    dc.onMessage((data) => {
      if (typeof data !== 'string' || data.length > MAX_MSG) return;
      let m;
      try { m = JSON.parse(data); } catch { return; }
      this.onMessage(peerId, m);
    });
    dc.onClosed(() => this.drop(peerId, c.pc));
  }

  drop(peerId, pc) {
    const c = this.conns.get(peerId);
    if (c && c.pc === pc) {
      this.conns.delete(peerId);
      if (c.dc) emit({ kind: 'disconnect', from: this.id, to: peerId });
    }
    try { pc.close(); } catch {}
  }

  onSignal(peerId, data) {
    if (!data || typeof data !== 'object') return;
    let c = this.conns.get(peerId);
    const desc = data.description;
    try {
      if (desc?.type === 'offer') {
        if (c) {
          // Glare: the lower id's offer wins (same rule as the extension).
          if (!c.dc?.isOpen() && this.id < peerId) return;
          this.drop(peerId, c.pc);
        }
        c = this.create(peerId);
        const cc = c;
        cc.pc.onDataChannel((dc) => this.bind(peerId, cc, dc));
        cc.pc.setRemoteDescription(desc.sdp, desc.type);
        this.flush(cc);
      } else if (desc?.type === 'answer') {
        if (c && !c.answered) {
          c.answered = true;
          c.pc.setRemoteDescription(desc.sdp, desc.type);
          this.flush(c);
        }
      } else if (data.candidate && c) {
        if (c.pc.remoteDescription()) c.pc.addRemoteCandidate(data.candidate.candidate, data.candidate.sdpMid ?? '0');
        else c.pending.push(data.candidate);
      }
    } catch {}
  }

  flush(c) {
    for (const cand of c.pending.splice(0)) {
      try { c.pc.addRemoteCandidate(cand.candidate, cand.sdpMid ?? '0'); } catch {}
    }
  }

  send(peerId, obj) {
    const dc = this.conns.get(peerId)?.dc;
    if (dc?.isOpen()) dc.sendMessage(JSON.stringify(obj));
  }

  sendHosts(peerId, hello) {
    const c = this.conns.get(peerId);
    if (!c) return;
    const fresh = [...this.records].filter(([id, ts]) => c.sentHosts.get(id) !== ts);
    if (!fresh.length && !hello) return;
    for (let i = 0; i === 0 || i < fresh.length; i += BATCH) {
      const slice = fresh.slice(i, i + BATCH);
      for (const [id, ts] of slice) c.sentHosts.set(id, ts);
      const hosts = slice.map(([id, ts]) => {
        const k = id.lastIndexOf(',');
        return [id.slice(0, k), id.slice(k + 1), ts];
      });
      this.send(peerId, { type: 'hosts', hosts, hello, last: i + BATCH >= fresh.length });
      emit({ kind: 'hosts', from: this.id, to: peerId, count: hosts.length, sample: hosts.slice(0, 3).map((h) => h[0]) });
    }
  }

  sendPeerList(peerId) {
    const c = this.conns.get(peerId);
    if (!c?.sharePeers) return;
    const rows = [...this.peers].filter(([id]) => id !== peerId && id !== this.id && !c.sentPeers.has(id));
    for (let i = 0; i < rows.length; i += BATCH) {
      const slice = rows.slice(i, i + BATCH);
      slice.forEach(([id]) => c.sentPeers.add(id));
      this.send(peerId, { type: 'peers', peers: slice.map(([id, ip]) => ({ id, ip })) });
      emit({ kind: 'peers', from: this.id, to: peerId, count: slice.length });
    }
  }

  onMessage(peerId, m) {
    const c = this.conns.get(peerId);
    if (!c) return;
    if (m.type === 'hosts' && Array.isArray(m.hosts)) {
      let added = 0;
      for (const e of m.hosts.slice(0, BATCH * 2)) {
        const [domain, ip, ts] = Array.isArray(e) ? e : [];
        if (typeof domain !== 'string' || typeof ip !== 'string' || typeof ts !== 'string') continue;
        const id = `${domain.toLowerCase()},${ip}`;
        c.sentHosts.set(id, ts); // never echo back to the source
        if (!this.records.has(id) || this.records.get(id) < ts) {
          this.records.set(id, ts);
          added++;
        }
      }
      emit({ kind: 'recv-hosts', from: peerId, to: this.id, count: m.hosts.length, added });
      if (added) this.broadcast();
    } else if (m.type === 'getPeers') {
      c.sharePeers = true;
      this.sendPeerList(peerId);
    } else if (m.type === 'peers' && Array.isArray(m.peers)) {
      for (const p of m.peers.slice(0, BATCH)) {
        if (typeof p?.id === 'string' && p.id.length <= 64 && typeof p.ip === 'string' && p.id !== this.id) this.peers.set(p.id, p.ip);
      }
      emit({ kind: 'recv-peers', from: peerId, to: this.id, count: m.peers.length });
      this.connectAll();
    }
  }

  broadcast() {
    for (const peerId of this.conns.keys()) {
      this.sendHosts(peerId, false);
      this.sendPeerList(peerId);
    }
  }

  async visitLoop() {
    if (this.closed) return;
    const domain = SITES[Math.floor(Math.random() * SITES.length)];
    try {
      const ips = await dns.resolve4(domain);
      const ip = ips[Math.floor(Math.random() * ips.length)];
      const id = `${domain},${ip}`;
      if (!this.records.has(id)) {
        this.records.set(id, new Date().toISOString());
        emit({ kind: 'visit', id: this.id, domain, ip });
        this.broadcast();
      }
    } catch {}
    this.visitTimer = setTimeout(() => this.visitLoop(), VISIT_MS * (0.5 + Math.random()));
  }

  // Asks every connected peer for its peer list, like an extension that has left the root server.
  askPeers() {
    for (const peerId of this.conns.keys()) this.send(peerId, { type: 'getPeers' });
  }

  close() {
    this.closed = true;
    clearTimeout(this.visitTimer);
    clearInterval(this.retryTimer);
    for (const [peerId, c] of [...this.conns]) this.drop(peerId, c.pc);
    this.ws?.close();
  }
}

server.listen(DASH_PORT, () => {
  console.log(`Dashboard: http://localhost:${DASH_PORT}`);
  console.log(`Spawning ${PEERS} dummy peers against ${SERVER_URL} (in-memory only)`);
  for (let i = 0; i < PEERS; i++) dummies.push(new Dummy(i + 1));
  setInterval(() => dummies.forEach((d) => d.askPeers()), 20000).unref();
});

function shutdown() {
  dummies.forEach((d) => d.close());
  nodeDataChannel.cleanup();
  server.close();
  process.exit(0);
}
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
