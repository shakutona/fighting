// ws_server_2_3.js — 2D対戦格闘ゲーム用 HTTP + WebSocket ロビー＆自動マッチングサーバー
// 外部ライブラリ不要（Node.js標準機能のみで動作）
//
// 【機能】
//  - ロビー形式：待機中プレイヤー一覧（ユーザー名・使用キャラ）をリアルタイム配信し、相手を選んで対戦可能
//  - 互換用の自動マッチング(quick)／ルーム指定(join)も維持
//  - TCP_NODELAY + keepAlive で中継遅延を最小化
//  - 20秒ごとに WebSocket Ping を送り、Render 等のプロキシによる無通信切断を防止

const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const os = require('os');

const PORT = Number(process.env.PORT) || 8080;
const WS_GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
const MAX_FRAME_BYTES = 256 * 1024;
const PING_INTERVAL_MS = 20000;
const DEAD_AFTER_MS = 70000;

let quickWaiter = null;
const rooms = new Map();
const clients = new Set();
let totalMatches = 0;
let nextClientId = 1;

function writeFrame(client, opcode, payload) {
  if (!client || client.closed || client.socket.destroyed) return;
  const len = payload.length;
  let header;
  if (len < 126) {
    header = Buffer.alloc(2);
    header[0] = 0x80 | opcode;
    header[1] = len;
  } else if (len < 65536) {
    header = Buffer.alloc(4);
    header[0] = 0x80 | opcode;
    header[1] = 126;
    header.writeUInt16BE(len, 2);
  } else {
    header = Buffer.alloc(10);
    header[0] = 0x80 | opcode;
    header[1] = 127;
    header.writeBigUInt64BE(BigInt(len), 2);
  }
  try {
    client.socket.write(len ? Buffer.concat([header, payload]) : header);
  } catch (e) {
    cleanupClient(client);
  }
}

function sendText(client, text) {
  writeFrame(client, 0x1, Buffer.from(text, 'utf8'));
}

function sendJson(client, obj) {
  sendText(client, JSON.stringify(obj));
}

function sendPong(client, payload) {
  writeFrame(client, 0xA, payload.subarray(0, 125));
}

function sendPing(client) {
  writeFrame(client, 0x9, Buffer.alloc(0));
}

function getLobbyUsers() {
  const list = [];
  for (const c of clients) {
    if (c.closed || c.socket.destroyed) continue;
    if (c.inLobby || c.partner) {
      list.push({
        id: c.id,
        name: c.name || '名無し',
        cid: c.cid | 0,
        status: c.partner ? 'playing' : 'waiting'
      });
    }
  }
  return list;
}

function broadcastLobby() {
  const users = getLobbyUsers();
  for (const c of clients) {
    if (!c.closed && c.inLobby && !c.partner) {
      sendJson(c, { t: 'lobby_list', selfId: c.id, users });
    }
  }
}

function cleanupClient(client) {
  if (client.closed) return;
  client.closed = true;
  client.inLobby = false;
  clients.delete(client);

  if (quickWaiter === client) {
    quickWaiter = null;
  }

  if (client.partner) {
    const p = client.partner;
    client.partner = null;
    p.partner = null;
    sendJson(p, { t: 'peer_left' });
  }

  if (client.room && rooms.has(client.room)) {
    const r = rooms.get(client.room);
    if (r.p1 === client) r.p1 = null;
    if (r.p2 === client) r.p2 = null;
    if (!r.p1 && !r.p2) rooms.delete(client.room);
  }

  try { client.socket.destroy(); } catch (e) { }
  broadcastLobby();
}

function pairClients(p1, p2) {
  p1.partner = p2;
  p2.partner = p1;
  p1.inLobby = false;
  p2.inLobby = false;
  totalMatches++;
  sendJson(p1, { t: 'joined', role: 1 });
  sendJson(p2, { t: 'joined', role: 2 });
  sendJson(p1, { t: 'peer_joined' });
  broadcastLobby();
}

function findClientById(id) {
  for (const c of clients) {
    if (c.id === id && !c.closed && !c.socket.destroyed) return c;
  }
  return null;
}

function handleMessage(client, rawText) {
  // 対戦中の入力・制御データはパース不要。最優先で相手へ即転送
  if (client.partner && !client.partner.closed) {
    if (
      rawText.charCodeAt(0) === 123 &&
      !rawText.startsWith('{"t":"quick"') &&
      !rawText.startsWith('{"t":"join"') &&
      !rawText.startsWith('{"t":"lobby_')
    ) {
      sendText(client.partner, rawText);
      return;
    }
  }

  let msg;
  try {
    msg = JSON.parse(rawText);
  } catch (e) {
    return;
  }
  if (!msg || typeof msg !== 'object') return;

  // ロビー入室
  if (msg.t === 'lobby_join') {
    if (client.partner) {
      const p = client.partner;
      client.partner = null;
      p.partner = null;
      sendJson(p, { t: 'peer_left' });
    }
    client.name = String(msg.name || '名無し').trim().slice(0, 12) || '名無し';
    client.cid = Number(msg.cid) || 0;
    client.inLobby = true;
    broadcastLobby();
    return;
  }

  // ロビー内の名前・キャラ更新
  if (msg.t === 'lobby_update') {
    if (msg.name !== undefined) {
      client.name = String(msg.name || '名無し').trim().slice(0, 12) || '名無し';
    }
    if (msg.cid !== undefined) {
      client.cid = Number(msg.cid) || 0;
    }
    broadcastLobby();
    return;
  }

  // ロビーで相手を選んで対戦申し込み
  if (msg.t === 'lobby_challenge') {
    if (client.partner) return;
    const targetId = Number(msg.targetId);
    const target = findClientById(targetId);
    if (!target || target === client || !target.inLobby || target.partner) {
      sendJson(client, { t: 'lobby_err', msg: '選択した相手はすでに対戦中か退出しました' });
      broadcastLobby();
      return;
    }
    if (msg.name !== undefined) {
      client.name = String(msg.name || '名無し').trim().slice(0, 12) || '名無し';
    }
    if (msg.cid !== undefined) {
      client.cid = Number(msg.cid) || 0;
    }
    pairClients(target, client);
    return;
  }

  // 番号なし自動マッチング（互換用）
  if (msg.t === 'quick') {
    client.name = String(msg.name || '名無し').trim().slice(0, 12) || '名無し';
    client.cid = Number(msg.cid) || 0;
    if (quickWaiter === client) return;
    if (quickWaiter && !quickWaiter.closed && !quickWaiter.socket.destroyed) {
      const host = quickWaiter;
      quickWaiter = null;
      pairClients(host, client);
    } else {
      quickWaiter = client;
      sendJson(client, { t: 'waiting' });
    }
    return;
  }

  // ルーム指定マッチング（互換用）
  if (msg.t === 'join') {
    const room = String(msg.room || '0000');
    client.room = room;
    let r = rooms.get(room);
    if (!r) {
      r = { p1: client, p2: null };
      rooms.set(room, r);
      sendJson(client, { t: 'joined', role: 1 });
    } else if (!r.p1) {
      r.p1 = client;
      sendJson(client, { t: 'joined', role: 1 });
      if (r.p2) pairClients(r.p1, r.p2);
    } else if (!r.p2 && r.p1 !== client) {
      r.p2 = client;
      pairClients(r.p1, r.p2);
    } else {
      sendJson(client, { t: 'err', msg: 'ルームが満員です' });
    }
    return;
  }

  // 相手がいる場合のその他のメッセージも転送
  if (client.partner && !client.partner.closed) {
    sendText(client.partner, rawText);
  }
}

const server = http.createServer((req, res) => {
  const urlPath = (req.url || '/').split('?')[0];
  if (urlPath === '/health') {
    const lobbyUsers = getLobbyUsers();
    res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store', 'Access-Control-Allow-Origin': '*' });
    res.end(JSON.stringify({
      ok: true,
      clients: clients.size,
      waiting: lobbyUsers.filter(u => u.status === 'waiting').length + (quickWaiter ? 1 : 0),
      lobby: lobbyUsers,
      matches: totalMatches,
      uptimeSec: Math.round(process.uptime())
    }));
    return;
  }
  if (urlPath === '/' || urlPath === '/index.html') {
    const htmlPath = path.join(__dirname, 'index.html');
    fs.readFile(htmlPath, (err, data) => {
      if (err) {
        res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
        res.end('index.html が見つかりません。ws_server_2_3.js と同じフォルダに index.html を置いてください。');
        return;
      }
      res.writeHead(200, {
        'Content-Type': 'text/html; charset=utf-8',
        'Cache-Control': 'no-store'
      });
      res.end(data);
    });
    return;
  }
  res.writeHead(404);
  res.end();
});

server.on('upgrade', (req, socket) => {
  const key = req.headers['sec-websocket-key'];
  if (!key) {
    socket.destroy();
    return;
  }

  socket.setNoDelay(true);
  socket.setKeepAlive(true, 20000);
  const acceptKey = crypto.createHash('sha1').update(key + WS_GUID).digest('base64');

  socket.write(
    'HTTP/1.1 101 Switching Protocols\r\n' +
    'Upgrade: websocket\r\n' +
    'Connection: Upgrade\r\n' +
    `Sec-WebSocket-Accept: ${acceptKey}\r\n\r\n`
  );

  const client = {
    id: nextClientId++,
    name: '名無し',
    cid: 0,
    inLobby: false,
    socket,
    buf: Buffer.alloc(0),
    closed: false,
    partner: null,
    room: null,
    lastSeen: Date.now()
  };
  clients.add(client);

  socket.on('data', chunk => {
    client.lastSeen = Date.now();
    client.buf = client.buf.length ? Buffer.concat([client.buf, chunk]) : chunk;

    while (client.buf.length >= 2) {
      const b0 = client.buf[0];
      const b1 = client.buf[1];
      const opcode = b0 & 0x0f;
      const masked = (b1 & 0x80) !== 0;
      let payloadLen = b1 & 0x7f;
      let offset = 2;

      if (payloadLen === 126) {
        if (client.buf.length < 4) break;
        payloadLen = client.buf.readUInt16BE(2);
        offset = 4;
      } else if (payloadLen === 127) {
        if (client.buf.length < 10) break;
        payloadLen = Number(client.buf.readBigUInt64BE(2));
        offset = 10;
      }

      if (payloadLen > MAX_FRAME_BYTES) {
        cleanupClient(client);
        return;
      }

      const maskLen = masked ? 4 : 0;
      if (client.buf.length < offset + maskLen + payloadLen) break;

      let payload = client.buf.subarray(offset + maskLen, offset + maskLen + payloadLen);
      if (masked) {
        const mask = client.buf.subarray(offset, offset + 4);
        const unmasked = Buffer.allocUnsafe(payloadLen);
        for (let i = 0; i < payloadLen; i++) {
          unmasked[i] = payload[i] ^ mask[i & 3];
        }
        payload = unmasked;
      }

      client.buf = client.buf.subarray(offset + maskLen + payloadLen);

      if (opcode === 0x8) {
        cleanupClient(client);
        return;
      } else if (opcode === 0x9) {
        sendPong(client, payload);
      } else if (opcode === 0x1) {
        handleMessage(client, payload.toString('utf8'));
      }
    }
  });

  socket.on('close', () => cleanupClient(client));
  socket.on('error', () => cleanupClient(client));
});

setInterval(() => {
  const now = Date.now();
  for (const c of clients) {
    if (now - c.lastSeen > DEAD_AFTER_MS) { cleanupClient(c); continue; }
    sendPing(c);
  }
}, PING_INTERVAL_MS).unref();

server.keepAliveTimeout = 65000;

server.listen(PORT, '0.0.0.0', () => {
  const rawHost = os.hostname();
  const localHost = rawHost.endsWith('.local') ? rawHost : `${rawHost}.local`;
  console.log('======================================================');
  console.log(` 2D対戦格闘ゲーム ロビーサーバー起動完了 (ポート: ${PORT})`);
  console.log('------------------------------------------------------');
  console.log(` 【PC側(1P) URL】   http://localhost:${PORT}/?auto=local`);
  console.log(` 【スマホ側(2P) URL】 http://${localHost}:${PORT}/?auto=local`);

  const nets = os.networkInterfaces();
  for (const name of Object.keys(nets)) {
    for (const net of nets[name]) {
      if (net.family === 'IPv4' && !net.internal) {
        console.log(`   (IP直指定の場合:   http://${net.address}:${PORT}/?auto=local )`);
      }
    }
  }
  console.log('======================================================');
});
