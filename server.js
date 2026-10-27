// Stringbound - server delle stanze online (Pingu Studio)
// --------------------------------------------------------
// Fa solo da "postino": crea stanze con un codice di 4 lettere, abbina i giocatori
// e inoltra i messaggi tra loro. La partita la calcola il gioco di chi crea la stanza (host).
//
// Avvio locale:   npm install && npm start      (porta 8080, oppure variabile PORT)
// Su Render:      vedi docs/ONLINE.md
//
// Variabili opzionali:
//   PORT            porta (Render la imposta da solo)
//   FAKE_DELAY_MS   ritardo artificiale per provare connessioni lente (solo test)

const http = require("http");
const { WebSocketServer } = require("ws");

const PORT = parseInt(process.env.PORT || "8080", 10);
const FAKE_DELAY_MS = parseInt(process.env.FAKE_DELAY_MS || "0", 10);
const MAX_PLAYERS = 4;
const CODE_CHARS = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"; // niente I, O, 0, 1 (si confondono)
const PROTOCOL = 1;

const rooms = new Map(); // code -> { code, public, started, players: Map(id -> ws), nextId, created }
let totalConnections = 0;

function makeCode() {
  for (let tries = 0; tries < 1000; tries++) {
    let c = "";
    for (let i = 0; i < 4; i++) c += CODE_CHARS[Math.floor(Math.random() * CODE_CHARS.length)];
    if (!rooms.has(c)) return c;
  }
  throw new Error("nessun codice libero");
}

function send(ws, obj) {
  if (ws.readyState !== 1) return;
  const text = typeof obj === "string" ? obj : JSON.stringify(obj);
  if (FAKE_DELAY_MS > 0) setTimeout(() => ws.readyState === 1 && ws.send(text), FAKE_DELAY_MS);
  else ws.send(text);
}

function roomInfo(room) {
  return { t: "room", code: room.code, players: [...room.players.keys()], started: room.started };
}

function broadcast(room, obj, exceptId = -1) {
  const text = JSON.stringify(obj);
  for (const [id, peer] of room.players) if (id !== exceptId) send(peer, text);
}

function leave(ws) {
  const room = ws.room;
  if (!room) return;
  room.players.delete(ws.pid);
  ws.room = null;
  if (ws.pid === 1 || room.players.size === 0) {
    // L'host è uscito: la stanza si chiude per tutti.
    broadcast(room, { t: "closed", reason: "host_left" });
    for (const peer of room.players.values()) peer.room = null;
    rooms.delete(room.code);
  } else {
    broadcast(room, { t: "peer_left", id: ws.pid });
    broadcast(room, roomInfo(room));
  }
}

function joinRoom(ws, room) {
  if (room.players.size >= MAX_PLAYERS) return send(ws, { t: "error", e: "full" });
  if (room.started) return send(ws, { t: "error", e: "started" });
  // Assegna il primo id libero (1 = host)
  let id = 1;
  while (room.players.has(id)) id++;
  ws.pid = id;
  ws.room = room;
  room.players.set(id, ws);
  send(ws, { t: "joined", code: room.code, id });
  broadcast(room, { t: "peer_joined", id }, id);
  broadcast(room, roomInfo(room));
}

function handle(ws, raw) {
  let msg;
  try { msg = JSON.parse(raw); } catch { return; }
  if (!msg || typeof msg.t !== "string") return;
  switch (msg.t) {
    case "hello":
      return send(ws, { t: "hello", protocol: PROTOCOL });
    case "create": {
      leave(ws);
      const room = { code: makeCode(), public: !!msg.public, started: false, players: new Map(), created: Date.now() };
      rooms.set(room.code, room);
      return joinRoom(ws, room);
    }
    case "join": {
      leave(ws);
      const code = String(msg.code || "").toUpperCase().trim();
      const room = rooms.get(code);
      if (!room) return send(ws, { t: "error", e: "not_found" });
      return joinRoom(ws, room);
    }
    case "quick": {
      leave(ws);
      // Stanza pubblica con posto libero e non ancora iniziata, altrimenti ne crea una
      for (const room of rooms.values()) {
        if (room.public && !room.started && room.players.size < MAX_PLAYERS) return joinRoom(ws, room);
      }
      const room = { code: makeCode(), public: true, started: false, players: new Map(), created: Date.now() };
      rooms.set(room.code, room);
      return joinRoom(ws, room);
    }
    case "start":
      if (ws.room && ws.pid === 1) { ws.room.started = true; broadcast(ws.room, roomInfo(ws.room)); }
      return;
    case "reopen":
      if (ws.room && ws.pid === 1) { ws.room.started = false; broadcast(ws.room, roomInfo(ws.room)); }
      return;
    case "leave":
      leave(ws);
      return send(ws, { t: "left" });
    case "msg": {
      // Inoltro dati di gioco: to = 0 tutti (tranne mittente), altrimenti id specifico
      const room = ws.room;
      if (!room) return;
      const out = { t: "msg", from: ws.pid, d: msg.d };
      if (msg.to && room.players.has(msg.to)) send(room.players.get(msg.to), out);
      else broadcast(room, out, ws.pid);
      return;
    }
    case "ping":
      return send(ws, { t: "pong", c: msg.c });
  }
}

// Server HTTP: risponde "ok" (Render lo usa per capire che il servizio è vivo)
const server = http.createServer((req, res) => {
  res.writeHead(200, { "Content-Type": "text/plain" });
  res.end(`Stringbound server ok - stanze: ${rooms.size}\n`);
});

const wss = new WebSocketServer({ server, maxPayload: 64 * 1024 });

wss.on("connection", (ws) => {
  totalConnections++;
  ws.isAlive = true;
  ws.room = null;
  ws.pid = 0;
  ws.on("pong", () => (ws.isAlive = true));
  ws.on("message", (data) => handle(ws, data.toString()));
  ws.on("close", () => leave(ws));
  ws.on("error", () => leave(ws));
});

// Controllo connessioni morte ogni 20 s e pulizia stanze vecchie
setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) { ws.terminate(); continue; }
    ws.isAlive = false;
    ws.ping();
  }
  const now = Date.now();
  for (const [code, room] of rooms) {
    if (room.players.size === 0 || now - room.created > 12 * 3600 * 1000) rooms.delete(code);
  }
}, 20000);

server.listen(PORT, () => {
  console.log(`Stringbound server in ascolto sulla porta ${PORT}${FAKE_DELAY_MS ? ` (ritardo finto ${FAKE_DELAY_MS} ms)` : ""}`);
});
