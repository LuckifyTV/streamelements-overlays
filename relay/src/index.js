
/* ================================================================
   Multistream Chat Overlay — Kick relay (Railway)
   ----------------------------------------------------------------
   Kick chat is NOT delivered natively by StreamElements, so this
   small service bridges it:

     Kick (Realtime WS) ──ingest──► relay ──push──► widget (WS)

   - Widgets connect to this server's WebSocket and send
       { type:'subscribe', platform:'kick', channel:'<slug-or-id>' }
   - The relay opens ONE Kick realtime connection per chatroom (shared
     across all widgets watching it) and forwards each chat message as
       { type:'message', payload:{...} }
     shaped exactly for the widget's normalizeKick().

   Kick connection is unofficial (reverse-engineered realtime channel);
   it is isolated here with reconnect/backoff so the widget never has
   to care. If Kick changes things, only this file needs updating.
   ================================================================ */
'use strict';

const http = require('http');
const { WebSocketServer, WebSocket } = require('ws');

const PORT = Number(process.env.PORT || 8080);
const HOST = '0.0.0.0';
const VERSION = 'v4';
const RELAY_TOKEN = process.env.RELAY_TOKEN || '';

// Kick's observed realtime endpoint. Unlike the former Pusher endpoint,
// this uses the Centrifugo JSON WebSocket protocol.
const KICK_REALTIME_URL = process.env.KICK_REALTIME_URL ||
  'wss://realtime.us-west-2.platform.kick.com/connection/websocket';

// The browser was observed POSTing to this endpoint from a private window.
// This is an unofficial endpoint and a cookie-free Railway request may fail.
const KICK_TOKEN_ENDPOINT =
  'https://web.kick.com/api/v1/realtime/auth/connection';

// Browser-observed client_id, which is NOT the chatroom ID. It may be session-
// or viewer-specific; change in Railway Variables if necessary.
const KICK_CLIENT_ID = process.env.KICK_CLIENT_ID || '102402';
const KICK_CONNECTION_TOKEN = process.env.KICK_CONNECTION_TOKEN || '';

async function getKickToken() {
  if (KICK_CONNECTION_TOKEN) {
    console.log('[kick] using configured connection token (value hidden)');
    return KICK_CONNECTION_TOKEN;
  }

  console.log('[kick] requesting anonymous realtime token (no cookies or login)');

  const response = await fetch(KICK_TOKEN_ENDPOINT, {
    method: 'POST',
    headers: {
      'Accept': 'application/json',
      'Content-Type': 'application/json',
      'Origin': 'https://kick.com',
      'Referer': 'https://kick.com/',
    },
    body: JSON.stringify({ client_id: KICK_CLIENT_ID }),
    signal: AbortSignal.timeout(12000),
  });

  if (!response.ok) {
    throw new Error(`anonymous token HTTP ${response.status}`);
  }

  const body = await response.json();
  const token = body && body.data && body.data.token;

  if (typeof token !== 'string' || !token) {
    throw new Error('anonymous token response missing data.token');
  }

  console.log('[kick] realtime token received (value hidden)');
  return token;
}

const BROWSERish = {
  'User-Agent':
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0 Safari/537.36',
  Accept: 'application/json',
};

// ---------------------------------------------------------------
// Kick chatroom registry — one realtime connection per chatroom.
// ---------------------------------------------------------------

/** chatroomId -> { ws, slug, subscribers, retry, alive, reconnectTimer, pingTimer } */
const rooms = new Map();

/** Resolve a channel slug to its numeric chatroom id (or accept an id directly). */
async function resolveChatroomId(channel) {
  const raw = String(channel || '').trim().toLowerCase();

  if (!raw) return null;

  if (/^\d+$/.test(raw)) return raw; // already a numeric chatroom id

  try {
    const res = await fetch(
      'https://kick.com/api/v2/channels/' + encodeURIComponent(raw),
      { headers: BROWSERish }
    );

    if (!res.ok) {
      console.warn(
        `[kick] channel lookup for "${raw}" returned ${res.status} ` +
        `(Cloudflare may be blocking the server) — pass the numeric chatroom id instead.`
      );
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

  room = {
    ws: null,
    slug: slug || chatroomId,
    subscribers: new Set(),
    retry: 0,
    alive: false
  };

  rooms.set(chatroomId, room);
  return room;
}

async function connectKick(chatroomId) {
  const room = rooms.get(chatroomId);

  if (!room || room.subscribers.size === 0) return;
  if (room.connecting) return;

  room.connecting = true;

  let token;

  try {
    token = await getKickToken();
  } catch (e) {
    console.warn('[kick] could not obtain connection token:', e.message);
    room.connecting = false;
    scheduleKickReconnect(chatroomId);
    return;
  }

  room.connecting = false;

  if (
    rooms.get(chatroomId) !== room ||
    room.subscribers.size === 0
  ) return;

  let ws;

  try {
    ws = new WebSocket(KICK_REALTIME_URL, {
      headers: { Origin: 'https://kick.com' },
      handshakeTimeout: 12000
    });
  } catch (e) {
    console.warn('[kick] could not create websocket:', e.message);
    scheduleKickReconnect(chatroomId);
    return;
  }

  room.ws = ws;
  room.alive = false;
  room.nextId = 1;

  function send(command) {
    if (ws.readyState !== WebSocket.OPEN) return;
    ws.send(JSON.stringify({ id: room.nextId++, ...command }));
  }

  ws.on('open', () => {
    if (room.ws !== ws) {
      ws.close();
      return;
    }

    send({
      connect: token
        ? { token, name: 'js' }
        : { name: 'js' }
    });

    console.log(
      `[kick] realtime socket open for chatroom ${chatroomId}; waiting for connect ack`
    );
  });

  ws.on('message', (buf) => {
    // Centrifugo may batch newline-delimited JSON replies in one WS frame.
    for (const line of buf.toString().split('\n')) {
      if (!line.trim()) continue;

      let frame;

      try {
        frame = JSON.parse(line);
      } catch {
        continue;
      }

      if (room.ws !== ws) return;

      if (frame.error) {
        console.warn(
          '[kick] realtime protocol error:',
          JSON.stringify(frame.error)
        );

        // Reconnect to fetch a fresh token on the next attempt.
        ws.close();
        continue;
      }

      if (frame.connect) {
        room.retry = 0;
        room.alive = true;

        // Observed in the authenticated browser session.
        // Chat publications may arrive on chatrooms.<id>.v2
        // after subscribing to chatrooms.<id>.
        for (const channel of [
          `chatroom_${chatroomId}`,
          `chatrooms.${chatroomId}`
        ]) {
          send({
            subscribe: {
              channel,
              flag: 1
            }
          });
        }

        console.log(
          `[kick] realtime connected; subscribing to chatroom ${chatroomId}`
        );
      }

      if (frame.subscribe) {
        console.log(
          `[kick] realtime subscription acknowledged for chatroom ${chatroomId}`
        );
      }

      if (frame.push && frame.push.pub) {
        const channel = String(frame.push.channel || '');

        if (!channel.includes(String(chatroomId))) continue;

        const pub = frame.push.pub.data;
        const event = pub && (pub.event || pub.type);
        const eventData = pub && (pub.data || pub.payload || pub);

        const ev = String(event || '').replace(/^App\\Events\\/, '');

        let data = eventData;

        try {
          if (typeof data === 'string') {
            data = JSON.parse(data);
          }
        } catch {
          continue;
        }

        if (
          ev === 'ChatMessageEvent' ||
          ev === 'ChatMessageSentEvent'
        ) {
          const payload = toUnifiedKick(data);

          if (payload) {
            broadcast(chatroomId, {
              type: 'message',
              payload
            });
          }
        } else {
          const alert = toUnifiedKickAlert(ev, data);

          if (alert) {
            broadcast(chatroomId, {
              type: 'alert',
              payload: alert
            });
          }
        }
      }

      // Empty JSON frames are protocol-level pings; reply with {}.
      if (
        Object.keys(frame).length === 0 &&
        ws.readyState === WebSocket.OPEN
      ) {
        ws.send('{}');
      }

      if (frame.disconnect) {
        console.warn(
          '[kick] realtime disconnected:',
          JSON.stringify(frame.disconnect)
        );

        ws.close();
      }
    }
  });

  ws.on('close', (code) => {
    if (room.ws !== ws) return;

    room.ws = null;
    room.alive = false;

    console.warn(
      `[kick] realtime socket closed (${code}); token, region or access may be invalid`
    );

    scheduleKickReconnect(chatroomId);
  });

  ws.on('error', (e) => {
    console.warn('[kick] realtime websocket error:', e.message);
  });
}

function scheduleKickReconnect(chatroomId) {
  const room = rooms.get(chatroomId);

  if (!room) return;

  if (room.subscribers.size === 0) {
    rooms.delete(chatroomId);
    return;
  }

  if (room.reconnectTimer) return;

  room.retry = Math.min(room.retry + 1, 8);

  const base = Math.min(
    1000 * Math.pow(2, room.retry),
    60000
  );

  room.reconnectTimer = setTimeout(() => {
    room.reconnectTimer = null;
    connectKick(chatroomId);
  }, base + Math.random() * base * 0.3);
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
    // Keep the badge text too (Kick badges have no image URL)
    // so the widget can both detect roles and render a badge.
    badges: (identity.badges || []).map((b) => ({
      type: b.type || '',
      text: b.text || ''
    })),
    emotes,
    text
  };
}

// Kick inlines emotes as "[emote:ID:NAME]".
// Convert to a {name:url} map and leave NAME in the text.
function parseKickContent(content) {
  const emotes = {};

  const text = String(content).replace(
    /\[emote:(\d+):([^\]]+)\]/g,
    (_, id, name) => {
      emotes[name] =
        `https://files.kick.com/emotes/${id}/fullsize`;

      return name;
    }
  );

  return { text, emotes };
}

// Best-effort mapping of unofficial Kick channel events → widget alerts.
function toUnifiedKickAlert(ev, data) {
  if (!data) return null;

  const name =
    data.username ||
    (data.user && data.user.username) ||
    (data.subscription &&
      data.subscription.user &&
      data.subscription.user.username) ||
    'Someone';

  switch (ev) {
    case 'SubscriptionEvent':
      return {
        type: 'sub',
        name,
        amount: data.months || 1
      };

    case 'GiftedSubscriptionsEvent':
    case 'LuckyUsersWhoGotGiftSubscriptionsEvent': {
      const ids = data.gifted_usernames || data.usernames || [];
      const count =
        data.gifted_amount ||
        (Array.isArray(ids) ? ids.length : 1);

      const sender = data.gifter_username || data.gifter || name;

      return {
        type: count > 1 ? 'communitygift' : 'gift',
        name: ids[0] || '',
        sender,
        count
      };
    }

    case 'StreamHostEvent':
      return {
        type: 'host',
        name: data.host_username || name,
        amount: data.number_viewers || ''
      };

    default:
      return null;
  }
}

// ---------------------------------------------------------------
// Widget-facing WebSocket server.
// ---------------------------------------------------------------

function broadcast(chatroomId, msg) {
  const room = rooms.get(chatroomId);

  if (!room) return;

  const data = JSON.stringify(msg);

  for (const sub of room.subscribers) {
    if (sub.readyState === WebSocket.OPEN) {
      sub.send(data);
    }
  }
}

function isAuthorizedSubscribe(msg, requiredToken) {
  if (!requiredToken) return true;

  return !!(
    msg &&
    msg.token &&
    String(msg.token) === String(requiredToken)
  );
}

function detachClientFromRooms(client, roomMap = rooms) {
  if (!client || !client._rooms) return;

  for (const id of client._rooms) {
    const room = roomMap.get(id);

    if (!room) continue;

    room.subscribers.delete(client);

    if (room.subscribers.size === 0) {
      if (room.reconnectTimer) {
        clearTimeout(room.reconnectTimer);
      }

      room.reconnectTimer = null;

      try {
        if (room.ws) room.ws.close();
      } catch {}

      roomMap.delete(id);
    }
  }

  client._rooms.clear();
}

// ---------------------------------------------------------------
// HTTP and WebSocket relay server.
// ---------------------------------------------------------------

// All server setup lives inside startServer() so requiring the
// file for tests does not open any sockets or timers.
function startServer() {
  console.log(
    `[relay] starting ${VERSION}; HTTP target ${HOST}:${PORT}`
  );

  const server = http.createServer((req, res) => {
    if (req.url === '/health' || req.url === '/') {
      res.writeHead(200, {
        'Content-Type': 'application/json'
      });

      res.end(
        JSON.stringify({
          ok: true,
          version: VERSION,
          rooms: rooms.size,
          uptime: process.uptime()
        })
      );

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
    console.log('[relay] widget websocket connected');

    const ip =
      (req.headers['x-forwarded-for'] || '')
        .split(',')[0]
        .trim() ||
      req.socket.remoteAddress;

    const count = (connectionCounts.get(ip) || 0) + 1;

    if (count > MAX_CONNS_PER_IP) {
      client.close(1008, 'Too many connections');
      return;
    }

    connectionCounts.set(ip, count);

    client._rooms = new Set();
    client.isAlive = true;

    client.on('pong', () => {
      client.isAlive = true;
    });

    client.on('message', async (buf) => {
      let msg;

      try {
        msg = JSON.parse(buf.toString());
      } catch {
        return;
      }

      if (
        msg &&
        msg.type === 'subscribe' &&
        msg.platform === 'kick'
      ) {
        if (!isAuthorizedSubscribe(msg, RELAY_TOKEN)) {
          client.send(
            JSON.stringify({
              type: 'error',
              error: 'unauthorized'
            })
          );

          client.close(1008, 'Unauthorized');
          return;
        }

        console.log(
          '[relay] widget requested Kick subscription:',
          String(msg.channel).slice(0, 80)
        );

        const chatroomId = await resolveChatroomId(msg.channel);

        if (!chatroomId) {
          client.send(
            JSON.stringify({
              type: 'error',
              error: 'kick_channel_unresolved',
              channel: msg.channel
            })
          );

          return;
        }

        const room = ensureRoom(
          chatroomId,
          String(msg.channel)
        );

        room.subscribers.add(client);
        client._rooms.add(chatroomId);

        if (
          !room.ws &&
          !room.reconnectTimer &&
          !room.connecting
        ) {
          connectKick(chatroomId);
        }

        client.send(
          JSON.stringify({
            type: 'subscribed',
            platform: 'kick',
            chatroomId
          })
        );
      }
    });

    client.on('close', () => {
      console.log('[relay] widget websocket disconnected');

      const c = connectionCounts.get(ip) || 1;

      if (c <= 1) {
        connectionCounts.delete(ip);
      } else {
        connectionCounts.set(ip, c - 1);
      }

      detachClientFromRooms(client);
    });
  });

  // Drop dead widget sockets.
  const heartbeat = setInterval(() => {
    for (const client of wss.clients) {
      if (client.isAlive === false) {
        client.terminate();
        continue;
      }

      client.isAlive = false;

      try {
        client.ping();
      } catch {}
    }
  }, 30000);

  wss.on('close', () => {
    clearInterval(heartbeat);
  });

  server.on('error', (e) => {
    console.error('[relay] HTTP listen error:', e.message);
  });

  server.listen(PORT, HOST, () => {
    console.log(
      `[relay] listening on ${HOST}:${PORT} (${VERSION})`
    );
  });

  return server;
}

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
