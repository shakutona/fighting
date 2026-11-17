// ws_server_2.js — 2D対戦格闘ゲーム用 HTTP + WebSocket 自動マッチングサーバー
// 外部ライブラリ不要（Node.js標準機能のみで動作）

const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const os = require('os');

const PORT = Number(process.env.PORT) || 8080;
const WS_GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';

let quickWaiter = null; // 番号なし即マッチ待機中のクライアント
const rooms = new Map(); // 互換用ルーム管理

function sendJson(client, obj) {
  sendText(client, JSON.stringify(obj));
}

function sendText(client, text) {
  if (!client || client.closed || client.socket.destroyed) return;
  const payload = Buffer.from(text, 'utf8');
  const len = payload.length;
  let header;

  if (len < 126) {
    header = Buffer.alloc(2);
    header[0] = 0x81;
    header[1] = len;
  } else if (len < 65536) {
    header = Buffer.alloc(4);
    header[0] = 0x81;
    header[1] = 126;
    header.writeUInt16BE(len, 2);
  } else {
    header = Buffer.alloc(10);
    header[0] = 0x81;
    header[1] = 127;
    header.writeBigUInt64BE(BigInt(len), 2);
  }

  try {
    client.socket.write(Buffer.concat([header, payload]));
  } catch (e) {
    cleanupClient(client);
  }
}

function sendPong(client, payload) {
  if (!client || client.closed || client.socket.destroyed) return;
  const len = Math.min(payload.length, 125);
  const header = Buffer.from([0x8a, len]);
  try {
    client.socket.write(Buffer.concat([header, payload.subarray(0, len)]));
  } catch (e) {
    cleanupClient(client);
  }
}

function cleanupClient(client) {
  if (client.closed) return;
  client.closed = true;

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
}

function pairClients(p1, p2) {
  p1.partner = p2;
  p2.partner = p1;
  sendJson(p1, { t: 'joined', role: 1 });
  sendJson(p2, { t: 'joined', role: 2 });
  sendJson(p1, { t: 'peer_joined' });
}

function handleMessage(client, rawText) {
  let msg;
  try {
    msg = JSON.parse(rawText);
  } catch (e) {
    return;
  }
  if (!msg || typeof msg !== 'object') return;

  // 番号なし自動マッチング
  if (msg.t === 'quick') {
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

  // 対戦中の入力・制御データを相手へ即転送
  if (client.partner && !client.partner.closed) {
    sendText(client.partner, rawText);
  }
}

// HTTPサーバー：同フォルダの index.html を配信（Type-C直結スマホ用）
const server = http.createServer((req, res) => {
  const urlPath = (req.url || '/').split('?')[0];
  if (urlPath === '/health') {
    res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
    res.end('OK');
    return;
  }
  if (urlPath === '/' || urlPath === '/index.html') {
    const htmlPath = path.join(__dirname, 'index.html');
    fs.readFile(htmlPath, (err, data) => {
      if (err) {
        res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
        res.end('index.html が見つかりません。ws_server_2.js と同じフォルダに index.html を置いてください。');
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

// WebSocketアップグレード処理
server.on('upgrade', (req, socket) => {
  const key = req.headers['sec-websocket-key'];
  if (!key) {
    socket.destroy();
    return;
  }

  socket.setNoDelay(true); // 低遅延化（Nagleアルゴリズム無効）
  const acceptKey = crypto.createHash('sha1').update(key + WS_GUID).digest('base64');

  socket.write(
    'HTTP/1.1 101 Switching Protocols\r\n' +
    'Upgrade: websocket\r\n' +
    'Connection: Upgrade\r\n' +
    `Sec-WebSocket-Accept: ${acceptKey}\r\n\r\n`
  );

  const client = { socket, buf: Buffer.alloc(0), closed: false, partner: null, room: null };

  socket.on('data', chunk => {
    client.buf = Buffer.concat([client.buf, chunk]);
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

      const maskLen = masked ? 4 : 0;
      if (client.buf.length < offset + maskLen + payloadLen) break;

      let payload = client.buf.subarray(offset + maskLen, offset + maskLen + payloadLen);
      if (masked) {
        const mask = client.buf.subarray(offset, offset + 4);
        const unmasked = Buffer.alloc(payloadLen);
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

server.listen(PORT, '0.0.0.0', () => {
  const rawHost = os.hostname();
  const localHost = rawHost.endsWith('.local') ? rawHost : `${rawHost}.local`;
  console.log('======================================================');
  console.log(` 2D対戦格闘ゲーム サーバー起動完了 (ポート: ${PORT})`);
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
