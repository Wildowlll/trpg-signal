/**
 * TRPG Watch — WebSocket 시그널링 서버
 * Railway에서 실행되는 Node.js 서버
 */
const { WebSocketServer } = require('ws');
const http = require('http');

const PORT = process.env.PORT || 8080;

// 세션별 피어 관리: sessionId → Map(peerId → WebSocket)
const sessions = new Map();

// HTTP 서버 (Railway 헬스체크용)
const server = http.createServer((req, res) => {
  if (req.url === '/health') {
    let peers = 0;
    sessions.forEach(s => peers += s.size);
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: 'ok', sessions: sessions.size, peers }));
    return;
  }
  res.writeHead(200);
  res.end('TRPG Watch Signaling Server');
});

// WebSocket 서버
const wss = new WebSocketServer({ server });

wss.on('connection', (ws, req) => {
  const url = new URL(req.url, `http://localhost`);
  const sessionId = url.searchParams.get('session');
  const peerId    = url.searchParams.get('peer');

  if (!sessionId || !peerId) {
    ws.close(1008, 'session and peer required');
    return;
  }

  // 세션 등록
  if (!sessions.has(sessionId)) sessions.set(sessionId, new Map());
  const session = sessions.get(sessionId);
  session.set(peerId, ws);

  console.log(`[+] ${peerId} joined session ${sessionId} (총 ${session.size}명)`);

  // 입장 알림 (자신 제외)
  broadcast(session, peerId, { type: 'peer-join', peerId });

  ws.on('message', (data) => {
    try {
      const msg = JSON.parse(data.toString());
      msg.from = peerId;

      if (msg.to) {
        // 특정 피어에게 1:1 전달
        const target = session.get(msg.to);
        if (target && target.readyState === target.OPEN) {
          target.send(JSON.stringify(msg));
        }
      } else {
        // 전체 브로드캐스트 (자신 제외)
        broadcast(session, peerId, msg);
      }
    } catch (err) {
      console.error('msg parse error:', err.message);
    }
  });

  ws.on('close', () => {
    session.delete(peerId);
    broadcast(session, peerId, { type: 'peer-leave', peerId });
    if (session.size === 0) sessions.delete(sessionId);
    console.log(`[-] ${peerId} left session ${sessionId} (남은 ${session.size}명)`);
  });

  ws.on('error', (err) => {
    console.error(`WS error (${peerId}):`, err.message);
    session.delete(peerId);
    if (session.size === 0) sessions.delete(sessionId);
  });
});

function broadcast(session, fromId, msg) {
  const data = JSON.stringify(msg);
  session.forEach((ws, id) => {
    if (id !== fromId && ws.readyState === ws.OPEN) {
      try { ws.send(data); } catch (e) {}
    }
  });
}

server.listen(PORT, () => {
  console.log(`✅ TRPG Watch 시그널링 서버 실행 중 (포트 ${PORT})`);
});
