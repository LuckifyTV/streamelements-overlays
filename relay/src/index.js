
const KICK_TOKEN_ENDPOINT =
  'https://web.kick.com/api/v1/realtime/auth/connection';

// Observed in Firefox. This is not the chatroom ID.
const KICK_CLIENT_ID =
  process.env.KICK_CLIENT_ID || '102402';

const KICK_CONNECTION_TOKEN =
  process.env.KICK_CONNECTION_TOKEN || '';

async function getKickToken() {
  if (KICK_CONNECTION_TOKEN) {
    return KICK_CONNECTION_TOKEN;
  }

  console.log('[kick] requesting anonymous realtime token');

  const response = await fetch(KICK_TOKEN_ENDPOINT, {
    method: 'POST',
    headers: {
      'Accept': 'application/json',
      'Content-Type': 'application/json',
      'Origin': 'https://kick.com',
      'Referer': 'https://kick.com/',
    },
    body: JSON.stringify({
      client_id: KICK_CLIENT_ID,
    }),
    signal: AbortSignal.timeout(10000),
  });

  if (!response.ok) {
    throw new Error(
      `anonymous token HTTP ${response.status}`
    );
  }

  const body = await response.json();
  const token = body?.data?.token;

  if (!token) {
    throw new Error(
      'anonymous token response missing data.token'
    );
  }

  console.log('[kick] realtime token received (value hidden)');

  return token;
}
