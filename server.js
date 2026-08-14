/**
 * TRPG Watch — WebSocket 시그널링 서버
 * Railway에서 실행되는 Node.js 서버
 */
const { WebSocketServer } = require('ws');
const http = require('http');

const PORT = process.env.PORT || 8080;
const MAX_FILE_SIZE = 5 * 1024 * 1024;  // 5MB
const MAX_FILES_PER_SESSION = 20;
const FILE_TTL = 6 * 60 * 60 * 1000;   // 6시간 후 자동 삭제

// 세션별 피어 관리
const sessions = new Map();

// 파일 저장소: fileId → { data, type, name, sessionId, createdAt }
const fileStore = new Map();

// 오래된 파일 자동 삭제
setInterval(() => {
  const now = Date.now();
  for (const [id, f] of fileStore) {
    if (now - f.createdAt > FILE_TTL) fileStore.delete(id);
  }
}, 30 * 60 * 1000); // 30분마다 체크

// HTTP 서버
const server = http.createServer((req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, X-File-Name, X-Session-Id');

  if (req.method === 'OPTIONS') {
    res.writeHead(204); res.end(); return;
  }

  // 헬스체크
  if (req.url === '/health') {
    let peers = 0, gms = 0;
    sessions.forEach(s => { peers += s.size; if (s.has('GM')) gms++; });
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: 'ok', sessions: sessions.size, peers, gms, files: fileStore.size }));
    return;
  }

  // 파일 업로드: POST /upload
  if (req.method === 'POST' && req.url === '/upload') {
    const sessionId = req.headers['x-session-id'];
    const fileName  = decodeURIComponent(req.headers['x-file-name'] || 'audio');
    const fileType  = req.headers['content-type'] || 'audio/mpeg';

    if (!sessionId) { res.writeHead(400); res.end('session-id required'); return; }

    // 세션당 파일 수 제한
    const sessionFiles = [...fileStore.values()].filter(f => f.sessionId === sessionId);
    if (sessionFiles.length >= MAX_FILES_PER_SESSION) {
      res.writeHead(429); res.end('too many files'); return;
    }

    const chunks = [];
    let size = 0;
    req.on('data', chunk => {
      size += chunk.length;
      if (size > MAX_FILE_SIZE) {
        req.destroy();
        res.writeHead(413); res.end('file too large'); return;
      }
      chunks.push(chunk);
    });
    req.on('end', () => {
      const data = Buffer.concat(chunks);
      const fileId = 'f-' + Date.now() + '-' + Math.random().toString(36).slice(2, 7);
      fileStore.set(fileId, { data, type: fileType, name: fileName, sessionId, createdAt: Date.now() });
      console.log(`[upload] ${fileName} (${Math.round(data.length/1024)}KB) → ${fileId}`);
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ fileId, name: fileName }));
    });
    req.on('error', () => { res.writeHead(500); res.end(); });
    return;
  }

  // 파일 다운로드: GET /file/:fileId
  const m = req.url.match(/^\/file\/([^?]+)/);
  if (req.method === 'GET' && m) {
    const f = fileStore.get(m[1]);
    if (!f) { res.writeHead(404); res.end('not found'); return; }
    res.writeHead(200, {
      'Content-Type': f.type,
      'Content-Length': f.data.length,
      'Content-Disposition': `inline; filename="${encodeURIComponent(f.name)}"`,
      'Cache-Control': 'no-store',
    });
    res.end(f.data);
    return;
  }

  res.writeHead(200); res.end('TRPG Watch Signaling Server');
});


// WebSocket 서버
const wss = new WebSocketServer({ server, maxPayload: 64 * 1024 }); // 64KB

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

      console.log(`[msg] ${peerId} → ${msg.to||'ALL'} type=${msg.type} session=${sessionId}`);

      if (msg.to) {
        // 특정 피어에게 1:1 전달
        const target = session.get(msg.to);
        if (target && target.readyState === target.OPEN) {
          target.send(JSON.stringify(msg));
          console.log(`[→] delivered to ${msg.to}`);
        } else {
          console.log(`[!] target ${msg.to} not found or closed. session peers: ${[...session.keys()].join(',')}`);
        }
      } else {
        // 전체 브로드캐스트 (자신 제외)
        broadcast(session, peerId, msg);
        console.log(`[→] broadcast to ${session.size-1} peers`);
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
