
/*
 * Multistream Chat Overlay — Kick relay (Railway)
 *
 * Kick Realtime WS → relay → StreamElements widget
 *
 * Experimental replacement for the old Pusher relay.
 * The Kick realtime endpoint may require a valid token.
 */
'use strict';

const http = require('http');
const { WebSocketServer, WebSocket } = require('ws');

const PORT = process.env.PORT || 8080;
const RELAY_TOKEN = process.env.RELAY_TOKEN || '';

const KICK_REALTIME_URL =
  process.env.KICK_REALTIME_URL ||
  'wss://realtime.us-west-2.platform.kick.com/connection/websocket';

// Optional authentication configuration.
// Configure these privately in Railway Variables.
// NEVER place authentication tokens in public GitHub code.
const KICK_CONNECTION_TOKEN =
  process.env.KICK_CONNECTION_TOKEN || '';

const KICK_TOKEN_URL =
  process.env.KICK_TOKEN_URL || '';

async function getKickToken() {
  if (!KICK_TOKEN_URL) return KICK_CONNECTION_TOKEN;

  const url = new URL(KICK_TOKEN_URL);

  if (url.protocol !== 'https:') {
    throw new Error('KICK_TOKEN_URL must use HTTPS');
  }

  const response = await fetch(url, {
    signal: AbortSignal.timeout(10000),
  });

  if (!response.ok) {
    throw new Error(
      `Kick token endpoint HTTP ${response.status}`
    );
  }

  const body = await response.json();

  if (!body || typeof body.token !== 'string') {
    throw new Error('Token endpoint did not return {token}');
  }

  return body.token;
}

const BROWSERish = {
  'User-Agent':
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) ' +
    'AppleWebKit/537.36 (KHTML, like Gecko) ' +
    'Chrome/127.0 Safari/537.36',
  Accept: 'application/json',
};

// --------------------------------------------------
// Kick chatroom registry
// --------------------------------------------------

const rooms = new Map();

async function resolveChatroomId(channel) {
  const raw = String(channel || '').trim().toLowerCase();

  if (!raw) return null;

  // Numeric chatroom ID.
  if (/^\d+$/.test(raw)) return raw;

  try {
    const res = await fetch(
      'https://kick.com/api/v2/channels/' +
      encodeURIComponent(raw),
      { headers: BROWSERish }
    );

    if (!res.ok) {
      console.warn(
        `[kick] channel lookup for "${raw}" returned ` +
        `${res.status}. Use numeric chatroom ID instead.`
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
    alive: false,
    reconnectTimer: null,
    nextId: 1,
  };

  rooms.set(chatroomId, room);
  return room;
}

// --------------------------------------------------
// Kick realtime WebSocket
// --------------------------------------------------

async function connectKick(chatroomId) {
  const room = rooms.get(chatroomId);

  if (!room || room.subscribers.size === 0) return;

  let token;

  try {
    token = await getKickToken();
  } catch (e) {
    console.warn(
      '[kick] could not obtain connection token:',
      e.message
    );
    scheduleKickReconnect(chatroomId);
    return;
  }

  if (
    rooms.get(chatroomId) !== room ||
    room.subscribers.size === 0
  ) {
    return;
  }

  const ws = new WebSocket(KICK_REALTIME_URL, {
    headers: {
      Origin: 'https://kick.com',
    },
  });

  room.ws = ws;
  room.alive = false;
  room.nextId = 1;

  function send(command) {
    if (ws.readyState !== WebSocket.OPEN) return;

    ws.send(
      JSON.stringify({
        id: room.nextId++,
        ...command,
      })
    );
  }

  ws.on('open', () => {
    if (room.ws !== ws) {
      ws.close();
      return;
    }

    send({
      connect: token
        ? { token, name: 'js' }
        : { name: 'js' },
    });

    console.log(
      `[kick] realtime socket open for chatroom ` +
      `${chatroomId}; waiting for connect ack`
    );
  });

  ws.on('message', (buf) => {
    // Centrifugo may send newline-delimited JSON.
    const lines = buf.toString().split('\n');

    for (const line of lines) {
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
        continue;
      }

      if (frame.connect) {
        room.retry = 0;
        room.alive = true;

        // Channels observed in Kick's browser connection.
        const channels = [
          `chatroom_${chatroomId}`,
          `chatrooms.${chatroomId}`,
        ];

        for (const channel of channels) {
          send({
            subscribe: {
              channel,
              flag: 1,
            },
          });
        }

        console.log(
          `[kick] realtime connected; ` +
          `subscribing to chatroom ${chatroomId}`
        );
      }

      if (frame.subscribe) {
        console.log(
          `[kick] realtime subscription acknowledged ` +
          `for chatroom ${chatroomId}`
        );
      }

      if (frame.push && frame.push.pub) {
        const channel = String(
          frame.push.channel || ''
        );

        if (!channel.includes(String(chatroomId))) {
          continue;
        }

        const pub = frame.push.pub.data;

        const event =
          pub && (pub.event || pub.type);

        const eventData =
          pub && (pub.data || pub.payload || pub);

        const ev = String(event || '')
          .replace(/^App\\Events\\/, '');

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
              payload,
            });
          }
        } else {
          const alert = toUnifiedKickAlert(ev, data);

          if (alert) {
            broadcast(chatroomId, {
              type: 'alert',
              payload: alert,
            });
          }
        }
      }

      // Protocol-level heartbeat.
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

    room.alive = false;

    console.warn(
      `[kick] realtime socket closed (${code})`
    );

    scheduleKickReconnect(chatroomId);
  });

  ws.on('error', (e) => {
    console.warn(
      '[kick] realtime websocket error:',
      e.message
    );
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

// --------------------------------------------------
// Convert Kick chat messages
// --------------------------------------------------

function toUnifiedKick(data) {
  if (!data) return null;

  const sender = data.sender || {};
  const identity = sender.identity || {};

  const { text, emotes } = parseKickContent(
    data.content || ''
  );

  return {
    msgId: data.id,
    userId:
      sender.id ||
      sender.slug ||
      sender.username,
    displayName: sender.username || 'anon',
    color: identity.color || '',
    avatar: '',
    badges: (identity.badges || []).map((b) => ({
      type: b.type || '',
      text: b.text || '',
    })),
    emotes,
    text,
  };
}

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

function toUnifiedKickAlert(ev, data) {
  if (!data) return null;

  const name =
    data.username ||
    (data.user && data.user.username) ||
    (
      data.subscription &&
      data.subscription.user &&
      data.subscription.user.username
    ) ||
    'Someone';

  switch (ev) {
    case 'SubscriptionEvent':
      return {
        type: 'sub',
        name,
        amount: data.months || 1,
      };

    case 'GiftedSubscriptionsEvent':
    case 'LuckyUsersWhoGotGiftSubscriptionsEvent': {
      const ids =
        data.gifted_usernames ||
        data.usernames ||
        [];

      const count =
        data.gifted_amount ||
        (Array.isArray(ids) ? ids.length : 1);

      const sender =
        data.gifter_username ||
        data.gifter ||
        name;

      return {
        type: count > 1
          ? 'communitygift'
          : 'gift',
        name: ids[0] || '',
        sender,
        count,
      };
    }

    case 'StreamHostEvent':
      return {
        type: 'host',
        name: data.host_username || name,
        amount: data.number_viewers || '',
      };

    default:
      return null;
  }
}

// --------------------------------------------------
// Send messages to StreamElements widgets
// --------------------------------------------------

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

function detachClientFromRooms(
  client,
  roomMap = rooms
) {
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

// --------------------------------------------------
// HTTP / WebSocket relay server
// --------------------------------------------------

function startServer() {
  const server = http.createServer((req, res) => {
    if (
      req.url === '/health' ||
      req.url === '/'
    ) {
      res.writeHead(200, {
        'Content-Type': 'application/json',
      });

      res.end(
        JSON.stringify({
          ok: true,
          rooms: rooms.size,
          uptime: process.uptime(),
        })
      );

      return;
    }

    res.writeHead(404);
    res.end();
  });

  const wss = new WebSocketServer({ server });

  const connectionCounts = new Map();
  const MAX_CONNS_PER_IP = 5;

  wss.on('connection', (client, req) => {
    const ip =
      (req.headers['x-forwarded-for'] || '')
        .split(',')[0]
        .trim() ||
      req.socket.remoteAddress;

    const count =
      (connectionCounts.get(ip) || 0) + 1;

    if (count > MAX_CONNS_PER_IP) {
      client.close(
        1008,
        'Too many connections'
      );
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
              error: 'unauthorized',
            })
          );

          client.close(
            1008,
            'Unauthorized'
          );

          return;
        }

        const chatroomId =
          await resolveChatroomId(msg.channel);

        if (!chatroomId) {
          client.send(
            JSON.stringify({
              type: 'error',
              error: 'kick_channel_unresolved',
              channel: msg.channel,
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
          !room.reconnectTimer
        ) {
          connectKick(chatroomId);
        }

        client.send(
          JSON.stringify({
            type: 'subscribed',
            platform: 'kick',
            chatroomId,
          })
        );
      }
    });

    client.on('close', () => {
      const c =
        connectionCounts.get(ip) || 1;

      if (c <= 1) {
        connectionCounts.delete(ip);
      } else {
        connectionCounts.set(ip, c - 1);
      }

      detachClientFromRooms(client);
    });
  });

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

  server.listen(PORT, () => {
    console.log(
      `[relay] listening on :${PORT}`
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
  startServer,
};
