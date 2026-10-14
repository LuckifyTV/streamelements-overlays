
/*
 * Kick Relay v4.1
 * StreamElements Multistream Chat Overlay
 *
 * Kick Realtime -> Railway -> StreamElements -> OBS
 *
 * Uses Kick's newer Centrifugo-style realtime service.
 * Keeps the original widget-facing WebSocket protocol.
 *
 * Experimental: Kick's internal API may change.
 */

'use strict';

const http = require('http');
const { WebSocketServer, WebSocket } = require('ws');

const PORT = Number(process.env.PORT || 8080);
const HOST = '0.0.0.0';
const VERSION = 'v4.1';

const RELAY_TOKEN = process.env.RELAY_TOKEN || '';

const KICK_REALTIME_URL =
  process.env.KICK_REALTIME_URL ||
  'wss://realtime.us-west-2.platform.kick.com/connection/websocket';

const KICK_TOKEN_ENDPOINT =
  'https://web.kick.com/api/v1/realtime/auth/connection';

const KICK_CLIENT_ID =
  process.env.KICK_CLIENT_ID || '102402';

const KICK_CONNECTION_TOKEN =
  process.env.KICK_CONNECTION_TOKEN || '';

const rooms = new Map();

const BROWSER_HEADERS = {
  'User-Agent':
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) ' +
    'AppleWebKit/537.36 (KHTML, like Gecko) ' +
    'Chrome/127.0 Safari/537.36',
  Accept: 'application/json'
};

// ====================================================
// KICK TOKEN
// ====================================================

async function getKickToken() {
  if (KICK_CONNECTION_TOKEN) {
    console.log('[kick] using configured token');
    return KICK_CONNECTION_TOKEN;
  }

  console.log('[kick] requesting anonymous realtime token');

  const response = await fetch(KICK_TOKEN_ENDPOINT, {
    method: 'POST',
    headers: {
      Accept: 'application/json',
      'Content-Type': 'application/json',
      Origin: 'https://kick.com',
      Referer: 'https://kick.com/'
    },
    body: JSON.stringify({
      client_id: KICK_CLIENT_ID
    }),
    signal: AbortSignal.timeout(12000)
  });

  if (!response.ok) {
    throw new Error(
      `Kick token HTTP ${response.status}`
    );
  }

  const json = await response.json();

  const token = json?.data?.token;

  if (!token || typeof token !== 'string') {
    throw new Error('Kick token missing from response');
  }

  console.log('[kick] realtime token received');

  return token;
}

// ====================================================
// CHATROOM RESOLUTION
// ====================================================

async function resolveChatroomId(channel) {
  const raw = String(channel || '').trim().toLowerCase();

  if (!raw) return null;

  if (/^\d+$/.test(raw)) {
    return raw;
  }

  try {
    const response = await fetch(
      'https://kick.com/api/v2/channels/' +
        encodeURIComponent(raw),
      {
        headers: BROWSER_HEADERS,
        signal: AbortSignal.timeout(10000)
      }
    );

    if (!response.ok) {
      console.warn(
        `[kick] channel lookup HTTP ${response.status}`
      );
      return null;
    }

    const json = await response.json();

    return json?.chatroom?.id
      ? String(json.chatroom.id)
      : null;

  } catch (error) {
    console.warn(
      '[kick] chatroom lookup error:',
      error.message
    );
    return null;
  }
}

// ====================================================
// ROOM MANAGEMENT
// ====================================================

function ensureRoom(chatroomId, slug) {
  let room = rooms.get(chatroomId);

  if (room) return room;

  room = {
    ws: null,
    slug: slug || chatroomId,
    subscribers: new Set(),
    retry: 0,
    alive: false,
    connecting: false,
    reconnectTimer: null,
    nextId: 1,
    pendingSubscriptions: new Map(),
    seenMessages: new Set()
  };

  rooms.set(chatroomId, room);

  return room;
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

  const delay = base + Math.random() * base * 0.3;

  console.log(
    `[kick] reconnect scheduled in ${Math.round(delay)}ms`
  );

  room.reconnectTimer = setTimeout(() => {
    room.reconnectTimer = null;
    connectKick(chatroomId);
  }, delay);
}

// ====================================================
// KICK REALTIME CONNECTION
// ====================================================

async function connectKick(chatroomId) {
  const room = rooms.get(chatroomId);

  if (!room || room.subscribers.size === 0) return;
  if (room.connecting || room.ws) return;

  room.connecting = true;

  let token;

  try {
    token = await getKickToken();
  } catch (error) {
    room.connecting = false;

    console.warn(
      '[kick] token request failed:',
      error.message
    );

    scheduleKickReconnect(chatroomId);
    return;
  }

  room.connecting = false;

  if (
    rooms.get(chatroomId) !== room ||
    room.subscribers.size === 0
  ) {
    return;
  }

  let ws;

  try {
    ws = new WebSocket(KICK_REALTIME_URL, {
      headers: {
        Origin: 'https://kick.com'
      },
      handshakeTimeout: 12000
    });
  } catch (error) {
    console.warn(
      '[kick] websocket creation failed:',
      error.message
    );

    scheduleKickReconnect(chatroomId);
    return;
  }

  room.ws = ws;
  room.alive = false;
  room.nextId = 1;
  room.pendingSubscriptions.clear();

  function send(command) {
    if (ws.readyState !== WebSocket.OPEN) {
      return null;
    }

    const id = room.nextId++;

    ws.send(JSON.stringify({
      id,
      ...command
    }));

    return id;
  }

  ws.on('open', () => {
    if (room.ws !== ws) {
      ws.close();
      return;
    }

    console.log(
      `[kick] socket open for chatroom ${chatroomId}`
    );

    send({
      connect: {
        token,
        name: 'js'
      }
    });
  });

  // --------------------------------------------------
  // INCOMING KICK MESSAGES
  // --------------------------------------------------

  ws.on('message', (buffer) => {
    if (room.ws !== ws) return;

    const lines = buffer.toString().split('\n');

    for (const line of lines) {
      if (!line.trim()) continue;

      let frame;

      try {
        frame = JSON.parse(line);
      } catch {
        console.warn('[kick] invalid JSON frame');
        continue;
      }

      // Centrifugo protocol heartbeat
      if (Object.keys(frame).length === 0) {
        if (ws.readyState === WebSocket.OPEN) {
          ws.send('{}');
        }
        continue;
      }

      // ----------------------------------------------
      // ERRORS
      // ----------------------------------------------

      if (frame.error) {
        const channel =
          room.pendingSubscriptions.get(frame.id);

        console.warn(
          '[kick] protocol error:',
          channel || 'connection',
          JSON.stringify(frame.error)
        );

        if (channel) {
          room.pendingSubscriptions.delete(frame.id);
        } else {
          ws.close();
        }

        continue;
      }

      // ----------------------------------------------
      // CONNECT ACKNOWLEDGEMENT
      // ----------------------------------------------

      if (frame.connect) {
        room.alive = true;
        room.retry = 0;

        console.log('[kick] realtime connected');

        // The browser was observed subscribing to
        // chatroom_<id> and chatrooms.<id>.
        // It received publications on chatrooms.<id>.v2.
        //
        // We now test all three channel names.

        const channels = [
          `chatroom_${chatroomId}`,
          `chatrooms.${chatroomId}`,
          `chatrooms.${chatroomId}.v2`
        ];

        for (const channel of channels) {
          const id = send({
            subscribe: {
              channel,
              flag: 1
            }
          });

          if (id !== null) {
            room.pendingSubscriptions.set(id, channel);
          }

          console.log(
            '[kick] requesting subscription:',
            channel
          );
        }

        continue;
      }

      // ----------------------------------------------
      // SUBSCRIPTION ACKNOWLEDGEMENT
      // ----------------------------------------------

      if (frame.subscribe) {
        const channel =
          room.pendingSubscriptions.get(frame.id) ||
          'unknown';

        room.pendingSubscriptions.delete(frame.id);

        console.log(
          '[kick] subscription accepted:',
          channel
        );

        continue;
      }

      // ----------------------------------------------
      // CHAT PUBLICATIONS
      // ----------------------------------------------

      if (frame.push && frame.push.pub) {
        const channel =
          String(frame.push.channel || '');

        console.log(
          '[kick] publication received:',
          channel
        );

        if (!channel.includes(String(chatroomId))) {
          continue;
        }

        let wrapper = frame.push.pub.data;

        try {
          if (typeof wrapper === 'string') {
            wrapper = JSON.parse(wrapper);
          }
        } catch {
          console.warn(
            '[kick] invalid publication payload'
          );
          continue;
        }

        if (!wrapper || typeof wrapper !== 'object') {
          continue;
        }

        const eventName = String(
          wrapper.event || wrapper.type || ''
        ).replace(/^App\\Events\\/, '');

        let eventData =
          wrapper.data || wrapper.payload || wrapper;

        try {
          if (typeof eventData === 'string') {
            eventData = JSON.parse(eventData);
          }
        } catch {
          console.warn('[kick] invalid nested event data');
          continue;
        }

        if (
          eventName === 'ChatMessageEvent' ||
          eventName === 'ChatMessageSentEvent'
        ) {
          const payload = toUnifiedKick(eventData);

          if (!payload) continue;

          // Prevent duplicates if Kick delivers one
          // publication on multiple subscribed channels.
          const msgId =
            payload.msgId != null
              ? String(payload.msgId)
              : '';

          if (msgId && room.seenMessages.has(msgId)) {
            continue;
          }

          if (msgId) {
            room.seenMessages.add(msgId);

            if (room.seenMessages.size > 500) {
              room.seenMessages.delete(
                room.seenMessages.values().next().value
              );
            }
          }

          broadcast(chatroomId, {
            type: 'message',
            payload
          });

          console.log(
            '[kick] chat forwarded to overlay'
          );

          continue;
        }

        const alert = toUnifiedKickAlert(
          eventName,
          eventData
        );

        if (alert) {
          broadcast(chatroomId, {
            type: 'alert',
            payload: alert
          });

          console.log(
            '[kick] alert forwarded:',
            eventName
          );
        }

        continue;
      }

      // ----------------------------------------------
      // DISCONNECT
      // ----------------------------------------------

      if (frame.disconnect) {
        console.warn(
          '[kick] server requested disconnect:',
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
      `[kick] realtime socket closed (${code})`
    );

    scheduleKickReconnect(chatroomId);
  });

  ws.on('error', (error) => {
    console.warn(
      '[kick] websocket error:',
      error.message
    );

    try {
      ws.close();
    } catch {}
  });
}

// ====================================================
// KICK MESSAGE NORMALIZATION
// ====================================================

function toUnifiedKick(data) {
  if (!data || typeof data !== 'object') {
    return null;
  }

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
    badges: (identity.badges || []).map((badge) => ({
      type: badge.type || '',
      text: badge.text || ''
    })),
    emotes,
    text
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
    data.user?.username ||
    data.subscription?.user?.username ||
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

// ====================================================
// WIDGET MESSAGE FORWARDING
// ====================================================

function broadcast(chatroomId, message) {
  const room = rooms.get(chatroomId);

  if (!room) return;

  const serialized = JSON.stringify(message);

  let delivered = 0;

  for (const client of room.subscribers) {
    if (client.readyState === WebSocket.OPEN) {
      client.send(serialized);
      delivered++;
    }
  }

  console.log(
    `[relay] message delivered to ${delivered} widget(s)`
  );
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

// ====================================================
// RAILWAY HTTP AND WEBSOCKET SERVER
// ====================================================

function startServer() {
  console.log(
    `[relay] starting ${VERSION} on ${HOST}:${PORT}`
  );

  const server = http.createServer((req, res) => {
    if (req.url === '/' || req.url === '/health') {
      res.writeHead(200, {
        'Content-Type': 'application/json'
      });

      res.end(JSON.stringify({
        ok: true,
        version: VERSION,
        rooms: rooms.size,
        uptime: process.uptime()
      }));

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
        .split(',')[0].trim() ||
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

    console.log(
      '[relay] widget websocket connected'
    );

    client.on('pong', () => {
      client.isAlive = true;
    });

    client.on('message', async (buffer) => {
      let msg;

      try {
        msg = JSON.parse(buffer.toString());
      } catch {
        return;
      }

      if (
        !msg ||
        msg.type !== 'subscribe' ||
        msg.platform !== 'kick'
      ) {
        return;
      }

      if (!isAuthorizedSubscribe(msg, RELAY_TOKEN)) {
        client.send(JSON.stringify({
          type: 'error',
          error: 'unauthorized'
        }));

        client.close(1008, 'Unauthorized');
        return;
      }

      console.log(
        '[relay] Kick subscription requested:',
        String(msg.channel).slice(0, 80)
      );

      const chatroomId =
        await resolveChatroomId(msg.channel);

      if (!chatroomId) {
        client.send(JSON.stringify({
          type: 'error',
          error: 'kick_channel_unresolved',
          channel: msg.channel
        }));

        return;
      }

      if (client.readyState !== WebSocket.OPEN) {
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
        !room.connecting &&
        !room.reconnectTimer
      ) {
        connectKick(chatroomId);
      }

      client.send(JSON.stringify({
        type: 'subscribed',
        platform: 'kick',
        chatroomId
      }));
    });

    client.on('close', () => {
      const count = connectionCounts.get(ip) || 1;

      if (count <= 1) {
        connectionCounts.delete(ip);
      } else {
        connectionCounts.set(ip, count - 1);
      }

      detachClientFromRooms(client);

      console.log(
        '[relay] widget websocket disconnected'
      );
    });
  });

  // Widget heartbeat
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

  server.on('error', (error) => {
    console.error(
      '[relay] HTTP server error:',
      error.message
    );
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
