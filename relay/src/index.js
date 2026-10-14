/* ================================================================
   Multistream Chat Overlay — Kick relay (Railway)
   ----------------------------------------------------------------
   Kick chat is NOT delivered natively by StreamElements, so this
   small service bridges it:

     Kick (Pusher WS)  ──ingest──►  relay  ──push──►  widget (WS)

   - Widgets connect to this server's WebSocket and send
       { type:'subscribe', platform:'kick', channel:'<slug-or-id>' }
   - The relay opens ONE Kick Pusher connection per chatroom (shared
     across all widgets watching it) and forwards each chat message as
       { type:'message', payload:{...} }
     shaped exactly for the widget's normalizeKick().

   Kick connection is unofficial (reverse-engineered Pusher channel);
   it is isolated here with reconnect/backoff so the widget never has
   to care. If Kick changes things, only this file needs updating.
   ================================================================ */
'use strict';

const http = require('http');
const { WebSocketServer, WebSocket } = require('ws');

const PORT = process.env.PORT || 8080;
const RELAY_TOKEN = process.env.RELAY_TOKEN || '';

// Kick's public Pusher app (same one kick.com uses in the browser).
const KICK_PUSHER_URL =
  'wss://ws-us2.pusher.com/app/32cbd69e4b950bf97679?protocol=7&client=js&version=8.4.0-rc2&flash=false';

const BROWSERish = {
  'User-Agent':
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0 Safari/537.36',
  Accept: 'application/json',
};

// ---------------------------------------------------------------
//  Kick chatroom registry — one Pusher connection per chatroom.
// ---------------------------------------------------------------
/** chatroomId -> { ws, slug, subscribers:Set<WebSocket>, retry, alive } */
const rooms = new Map();

/** Resolve a channel slug to its numeric chatroom id (or accept an id directly). */
async function resolveChatroomId(channel) {
  const raw = String(channel || '').trim().toLowerCase();
  if (!raw) return null;
  if (/^\d+$/.test(raw)) return raw; // already a numeric chatroom id
  try {
    const res = await fetch('https://kick.com/api/v2/channels/' + encodeURIComponent(raw), {
      headers: BROWSERish,
    });
    if (!res.ok) {
      console.warn(`[kick] channel lookup for "${raw}" returned ${res.status} ` +
        `(Cloudflare may be blocking the server) — pass the numeric chatroom id instead.`);
      return null;
    }
    const json = await res.json();
    const id = json && json.chatroom && json.chatroom.id;
    return id ? String(id) : null;
  } catch (e) {
    console.warn('[kick] channel lookup failed:', e.message);
    return null;
  }
}

function ensureRoom(chatroomId, slug) {
  let room = rooms.get(chatroomId);
  if (room) return room;
  room = { ws: null, slug: slug || chatroomId, subscribers: new Set(), retry: 0, alive: false };
  rooms.set(chatroomId, room);
  connectKick(chatroomId);
  return room;
}

function connectKick(chatroomId) {
  const room = rooms.get(chatroomId);
  if (!room) return;

  const ws = new WebSocket(KICK_PUSHER_URL);
  room.ws = ws;

  ws.on('open', () => {
    room.retry = 0;
    // Subscribe to BOTH the chatroom (chat messages) and the channel
    // (events: subs, gifts, hosts, raids) so we can surface Kick alerts too.
    ws.send(JSON.stringify({
      event: 'pusher:subscribe',
      data: { auth: '', channel: `chatrooms.${chatroomId}.v2` },
    }));
    ws.send(JSON.stringify({
      event: 'pusher:subscribe',
      data: { auth: '', channel: `channel.${chatroomId}` },
    }));
    console.log(`[kick] socket open → subscribing chatrooms.${chatroomId}.v2 (${room.slug})`);
  });

  ws.on('message', (buf) => {
    let frame;
    try { frame = JSON.parse(buf.toString()); } catch { return; }

    // Pusher housekeeping
    if (frame.event === 'pusher:ping') {
      ws.send(JSON.stringify({ event: 'pusher:pong', data: {} }));
      return;
    }
    if (frame.event === 'pusher:connection_established') return;
    if (frame.event === 'pusher_internal:subscription_succeeded') {
      room.alive = true;
      console.log(`[kick] subscribed ✓ ${frame.channel || chatroomId} (${room.slug})`);
      return;
    }
    if (frame.event === 'pusher:error') {
      console.warn('[kick] pusher error:', frame.data);
      return;
    }

    // Strip the "App\Events\" prefix so we can match the bare event name.
    const ev = String(frame.event || '').replace(/^App\\Events\\/, '');
    let data;
    try { data = typeof frame.data === 'string' ? JSON.parse(frame.data) : frame.data; }
    catch { return; }

    // Chat — Kick has used both names across versions; accept either.
    if (ev === 'ChatMessageEvent' || ev === 'ChatMessageSentEvent') {
      const payload = toUnifiedKick(data);
      if (payload) broadcast(chatroomId, { type: 'message', payload });
      return;
    }

    // Alerts (best-effort; channel events are unofficial and vary).
    const alert = toUnifiedKickAlert(ev, data);
    if (alert) broadcast(chatroomId, { type: 'alert', payload: alert });
  });

  ws.on('close', () => { room.alive = false; scheduleKickReconnect(chatroomId); });
  ws.on('error', (e) => { console.warn('[kick] ws error:', e && e.message); try { ws.close(); } catch {} });
}

function scheduleKickReconnect(chatroomId) {
  const room = rooms.get(chatroomId);
  if (!room) return;
  if (room.subscribers.size === 0) { rooms.delete(chatroomId); return; } // nobody left → drop
  room.retry = Math.min(room.retry + 1, 8);
  const base = Math.min(1000 * Math.pow(2, room.retry), 60000);
  const jitter = Math.random() * base * 0.3;
  setTimeout(() => connectKick(chatroomId), base + jitter);
}

// Kick ChatMessageEvent → widget normalizeKick() shape.
function toUnifiedKick(data) {
  if (!data) return null;
  const sender = data.sender || {};
  const identity = sender.identity || {};
  const { text, emotes } = parseKickContent(data.content || '');
  return {
    msgId: data.id,
    userId: sender.id || sender.slug || sender.username,
    displayName: sender.username || 'anon',
    color: identity.color || '',
    avatar: '',
    // Keep the badge text too (Kick badges have no image URL) so the widget can
    // both detect roles and, later, render a text/emoji badge if desired.
    badges: (identity.badges || []).map((b) => ({ type: b.type || '', text: b.text || '' })),
    emotes,
    text,
  };
}

// Kick inlines emotes as "[emote:ID:NAME]". Convert to a {name:url} map and
// leave the bare NAME in the text so the widget's renderText can swap it in.
function parseKickContent(content) {
  const emotes = {};
  const text = String(content).replace(/\[emote:(\d+):([^\]]+)\]/g, (_, id, name) => {
    emotes[name] = `https://files.kick.com/emotes/${id}/fullsize`;
    return name;
  });
  return { text, emotes };
}

// Best-effort mapping of unofficial Kick channel events → widget alert payload.
// Shapes vary; we read defensively and skip anything we don't recognise.
function toUnifiedKickAlert(ev, data) {
  if (!data) return null;
  const name = (data.username) ||
    (data.user && data.user.username) ||
    (data.subscription && data.subscription.user && data.subscription.user.username) || 'Someone';
  switch (ev) {
    case 'SubscriptionEvent':
      return { type: 'sub', name, amount: data.months || 1 };
    case 'GiftedSubscriptionsEvent':
    case 'LuckyUsersWhoGotGiftSubscriptionsEvent': {
      const ids = data.gifted_usernames || data.usernames || [];
      const count = data.gifted_amount || (Array.isArray(ids) ? ids.length : 1);
      const sender = data.gifter_username || data.gifter || name;
      return { type: count > 1 ? 'communitygift' : 'gift', name: ids[0] || '', sender, count };
    }
    case 'StreamHostEvent':
      return { type: 'host', name: data.host_username || name, amount: data.number_viewers || '' };
    default:
      return null;
  }
}

// ---------------------------------------------------------------
//  Widget-facing WebSocket server.
// ---------------------------------------------------------------
function broadcast(chatroomId, msg) {
  const room = rooms.get(chatroomId);
  if (!room) return;
  const data = JSON.stringify(msg);
  for (const sub of room.subscribers) {
    if (sub.readyState === WebSocket.OPEN) sub.send(data);
  }
}

function isAuthorizedSubscribe(msg, requiredToken) {
  if (!requiredToken) return true;
  return !!(msg && msg.token && String(msg.token) === String(requiredToken));
}

function detachClientFromRooms(client, roomMap = rooms) {
  if (!client || !client._rooms) return;
  for (const id of client._rooms) {
    const room = roomMap.get(id);
    if (!room) continue;
    room.subscribers.delete(client);
    if (room.subscribers.size === 0) {
      try { if (room.ws) room.ws.close(); } catch {}
      roomMap.delete(id);
    }
  }
  client._rooms.clear();
}

// All server setup lives inside startServer() so that `require`-ing this file
// (e.g. from relay/test.cjs) creates NO sockets, timers, or open handles — the
// process can exit cleanly. Only running it directly starts the server.
function startServer() {
  const server = http.createServer((req, res) => {
    if (req.url === '/health' || req.url === '/') {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: true, rooms: rooms.size, uptime: process.uptime() }));
      return;
    }
    res.writeHead(404);
    res.end();
  });

  const wss = new WebSocketServer({ server });

  // Rate limiting: max connections per IP
  const connectionCounts = new Map();
  const MAX_CONNS_PER_IP = 5;

  wss.on('connection', (client, req) => {
    const ip = (req.headers['x-forwarded-for'] || '').split(',')[0].trim()
             || req.socket.remoteAddress;
    const count = (connectionCounts.get(ip) || 0) + 1;
    if (count > MAX_CONNS_PER_IP) {
      client.close(1008, 'Too many connections');
      return;
    }
    connectionCounts.set(ip, count);

    client._rooms = new Set();
    client.isAlive = true;
    client.on('pong', () => { client.isAlive = true; });

    client.on('message', async (buf) => {
      let msg;
      try { msg = JSON.parse(buf.toString()); } catch { return; }
      if (msg && msg.type === 'subscribe' && msg.platform === 'kick') {
        if (!isAuthorizedSubscribe(msg, RELAY_TOKEN)) {
          client.send(JSON.stringify({ type: 'error', error: 'unauthorized' }));
          client.close(1008, 'Unauthorized');
          return;
        }
        const chatroomId = await resolveChatroomId(msg.channel);
        if (!chatroomId) {
          client.send(JSON.stringify({ type: 'error', error: 'kick_channel_unresolved', channel: msg.channel }));
          return;
        }
        const room = ensureRoom(chatroomId, String(msg.channel));
        room.subscribers.add(client);
        client._rooms.add(chatroomId);
        client.send(JSON.stringify({ type: 'subscribed', platform: 'kick', chatroomId }));
      }
    });

    client.on('close', () => {
      // Decrement rate-limit counter
      const c = connectionCounts.get(ip) || 1;
      if (c <= 1) connectionCounts.delete(ip);
      else connectionCounts.set(ip, c - 1);
      // Clean up room subscriptions
      detachClientFromRooms(client);
    });
  });

  // Drop dead widget sockets (Railway/proxies can leave zombies).
  const heartbeat = setInterval(() => {
    for (const client of wss.clients) {
      if (client.isAlive === false) { client.terminate(); continue; }
      client.isAlive = false;
      try { client.ping(); } catch {}
    }
  }, 30000);
  wss.on('close', () => clearInterval(heartbeat));

  server.listen(PORT, () => console.log(`[relay] listening on :${PORT}`));
  return server;
}

// Only start when run directly; exporting the pure parsers lets us unit-test
// them (see relay/test.cjs) without opening a socket or leaking timers.
if (require.main === module) {
  startServer();
}

module.exports = {
  parseKickContent,
  toUnifiedKick,
  toUnifiedKickAlert,
  isAuthorizedSubscribe,
  detachClientFromRooms,
  startServer
};
