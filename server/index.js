const crypto = require('crypto');
const { WebSocketServer, WebSocket } = require('ws');

const PORT = process.env.PORT || 8080;
const INTERVAL_MS = 15000;

const wss = new WebSocketServer({ port: PORT, maxPayload: 64 * 1024 });

const clientIp = (req) => {
  const ip = req.socket.remoteAddress || 'unknown';
  return ip.replace(/^::ffff:/, '');
};

const isOpen = (c) => c.readyState === WebSocket.OPEN;

function sendPeers() {
  for (const client of wss.clients) {
    if (!isOpen(client)) continue;
    const peers = [];
    for (const other of wss.clients) {
      if (other !== client && isOpen(other)) peers.push({ id: other.id, ip: other.ip });
    }
    client.send(JSON.stringify({ type: 'peers', peers }));
  }
}

wss.on('connection', (ws, req) => {
  ws.ip = clientIp(req);
  ws.id = crypto.randomUUID();
  console.log(`Client connected: ${ws.ip} (${ws.id})`);
  ws.send(JSON.stringify({ type: 'welcome', id: ws.id }));

  // Relays WebRTC signaling (offer/answer/ICE) between two clients; the payload is opaque.
  ws.on('message', (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return;
    }
    if (msg.type !== 'signal' || typeof msg.to !== 'string') return;
    for (const target of wss.clients) {
      if (target.id === msg.to && isOpen(target)) {
        target.send(JSON.stringify({ type: 'signal', from: ws.id, data: msg.data }));
        break;
      }
    }
  });

  ws.on('close', () => console.log(`Client disconnected: ${ws.ip}`));
  ws.on('error', (err) => console.error(`Socket error (${ws.ip}):`, err.message));
});

setInterval(sendPeers, INTERVAL_MS);

console.log(`WebSocket server listening on port ${PORT}`);
