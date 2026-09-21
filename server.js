require('dotenv').config();

const path = require('node:path');
const http = require('node:http');
const crypto = require('node:crypto');
const fs = require('node:fs');
const express = require('express');
const multer = require('multer');
const { Server } = require('socket.io');
const webpush = require('web-push');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: true, credentials: true },
  transports: ['websocket', 'polling'],
  pingInterval: 25000,
  pingTimeout: 20000,
});

const PORT = Number(process.env.PORT || 3000);
const APP_NAME = process.env.APP_NAME || 'Lutsa vidcall';
const MAX_ROOM_USERS = 2;
const MAX_FILE_SIZE = Number(process.env.MAX_FILE_SIZE || 25 * 1024 * 1024);
const MAX_CHAT_HISTORY = 100;

// Metered dynamic TURN settings. Secret key stays server-side and is never sent to the browser.
const METERED_APP_NAME = String(process.env.METERED_APP_NAME || '').trim();
const METERED_BASE_URL = String(process.env.METERED_BASE_URL || (METERED_APP_NAME ? `https://${METERED_APP_NAME}.metered.live` : '')).replace(/\/$/, '');
const METERED_SECRET_KEY = String(process.env.METERED_SECRET_KEY || '').trim();
const METERED_REGION = String(process.env.METERED_REGION || 'global').trim() || 'global';
const TURN_CREDENTIAL_TTL = Math.max(3600, Math.min(172800, Number(process.env.TURN_CREDENTIAL_TTL || 7200)));
const TURN_API_TIMEOUT_MS = Math.max(5000, Number(process.env.TURN_API_TIMEOUT_MS || 15000));
const TURN_EMPTY_ROOM_DISABLE_DELAY_MS = Math.max(10000, Number(process.env.TURN_EMPTY_ROOM_DISABLE_DELAY_MS || 30000));

const UPLOAD_DIR = path.join(__dirname, 'public', 'uploads');
fs.mkdirSync(UPLOAD_DIR, { recursive: true });
const DATA_DIR = path.join(__dirname, 'data');
const CHAT_HISTORY_FILE = path.join(DATA_DIR, 'chat-history.json');
const PUSH_SUBSCRIPTIONS_FILE = path.join(DATA_DIR, 'push-subscriptions.json');
fs.mkdirSync(DATA_DIR, { recursive: true });
const CHAT_HISTORY_TTL_DAYS = Math.max(1, Number(process.env.CHAT_HISTORY_TTL_DAYS || 30));
const PUSH_SUBSCRIBER_RETENTION_DAYS = Math.max(7, Number(process.env.PUSH_SUBSCRIBER_RETENTION_DAYS || 90));
const PUSH_CONTACT = String(process.env.PUSH_CONTACT || 'mailto:admin@example.com').trim();
let pushVapid = { publicKey: String(process.env.VAPID_PUBLIC_KEY || '').trim(), privateKey: String(process.env.VAPID_PRIVATE_KEY || '').trim() };
if ((!pushVapid.publicKey || !pushVapid.privateKey) && fs.existsSync(path.join(DATA_DIR, 'vapid.json'))) {
  try { pushVapid = JSON.parse(fs.readFileSync(path.join(DATA_DIR, 'vapid.json'), 'utf8')); } catch {}
}
if (!pushVapid.publicKey || !pushVapid.privateKey) {
  const generated = webpush.generateVAPIDKeys();
  pushVapid = generated;
  fs.writeFileSync(path.join(DATA_DIR, 'vapid.json'), JSON.stringify(pushVapid, null, 2), 'utf8');
}
webpush.setVapidDetails(PUSH_CONTACT, pushVapid.publicKey, pushVapid.privateKey);

function loadJson(file, fallback) {
  try { return JSON.parse(fs.readFileSync(file, 'utf8')); } catch { return fallback; }
}
function saveJsonAtomic(file, value) {
  const temp = `${file}.tmp`;
  fs.writeFileSync(temp, JSON.stringify(value, null, 2), 'utf8');
  fs.renameSync(temp, file);
}

const persistedHistory = loadJson(CHAT_HISTORY_FILE, {});
const persistedSubscriptions = loadJson(PUSH_SUBSCRIPTIONS_FILE, []);

const roomUsers = new Map();
const socketRoom = new Map();
const roomMessages = new Map();
const roomTurnSessions = new Map();
const pushSubscriptions = Array.isArray(persistedSubscriptions) ? persistedSubscriptions : [];
for (const [roomId, messages] of Object.entries(persistedHistory || {})) {
  if (Array.isArray(messages) && messages.length) roomMessages.set(normalizeRoomId(roomId), messages.slice(-MAX_CHAT_HISTORY));
}
function persistChatHistory() {
  const obj = {};
  for (const [roomId, messages] of roomMessages.entries()) obj[roomId] = messages.slice(-MAX_CHAT_HISTORY);
  saveJsonAtomic(CHAT_HISTORY_FILE, obj);
}
function pruneChatHistory() {
  const cutoff = Date.now() - CHAT_HISTORY_TTL_DAYS * 86400000;
  let changed = false;
  for (const [roomId, messages] of roomMessages.entries()) {
    const kept = messages.filter(m => (m.timestamp || 0) >= cutoff).slice(-MAX_CHAT_HISTORY);
    if (kept.length !== messages.length) changed = true;
    if (kept.length) roomMessages.set(roomId, kept); else roomMessages.delete(roomId);
  }
  if (changed) persistChatHistory();
}
pruneChatHistory();
function persistPushSubscriptions() { saveJsonAtomic(PUSH_SUBSCRIPTIONS_FILE, pushSubscriptions.slice(-1000)); }
function prunePushSubscriptions() {
  const cutoff = Date.now() - PUSH_SUBSCRIBER_RETENTION_DAYS * 86400000;
  let changed = false;
  for (let i = pushSubscriptions.length - 1; i >= 0; i--) {
    if ((pushSubscriptions[i].lastSeenAt || pushSubscriptions[i].createdAt || 0) < cutoff) { pushSubscriptions.splice(i, 1); changed = true; }
  }
  if (changed) persistPushSubscriptions();
}
prunePushSubscriptions();

const allowedExtensions = new Set([
  '.jpg', '.jpeg', '.png', '.webp', '.gif', '.bmp',
  '.pdf', '.txt', '.csv', '.doc', '.docx', '.xls', '.xlsx',
  '.ppt', '.pptx', '.zip', '.rar', '.7z', '.mp3', '.wav', '.m4a', '.ogg', '.webm', '.mp4'
]);

const upload = multer({
  storage: multer.diskStorage({
    destination: (_req, _file, cb) => cb(null, UPLOAD_DIR),
    filename: (_req, file, cb) => {
      const ext = path.extname(file.originalname || '').toLowerCase();
      const token = `${Date.now()}_${crypto.randomBytes(8).toString('hex')}`;
      cb(null, `${token}${ext}`);
    },
  }),
  limits: { fileSize: MAX_FILE_SIZE, files: 1 },
  fileFilter: (_req, file, cb) => {
    const ext = path.extname(file.originalname || '').toLowerCase();
    if (!allowedExtensions.has(ext)) return cb(new Error('FILE_TYPE_NOT_ALLOWED'));
    cb(null, true);
  },
});


function meteredConfigured() {
  return Boolean(METERED_BASE_URL && METERED_SECRET_KEY);
}

function publicIceServersOnly() {
  const urls = String(process.env.STUN_URL || '').trim();
  return urls ? [{ urls }] : [];
}

function buildFallbackIceServers() {
  const turnUrls = String(process.env.TURN_URLS || '').split(',').map(s => s.trim()).filter(Boolean);
  const fallback = publicIceServersOnly();
  if (turnUrls.length && process.env.TURN_USERNAME && process.env.TURN_CREDENTIAL) {
    fallback.push({ urls: turnUrls, username: String(process.env.TURN_USERNAME), credential: String(process.env.TURN_CREDENTIAL), credentialType: 'password' });
  }
  return fallback;
}

async function meteredRequest(pathname, options = {}) {
  if (!meteredConfigured()) throw new Error('METERED_NOT_CONFIGURED');
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), TURN_API_TIMEOUT_MS);
  try {
    const response = await fetch(`${METERED_BASE_URL}${pathname}`, { ...options, signal: controller.signal, headers: { 'Content-Type': 'application/json', ...(options.headers || {}) } });
    const text = await response.text();
    let body = {};
    try { body = text ? JSON.parse(text) : {}; } catch { body = { raw: text }; }
    if (!response.ok) {
      const message = body?.message || body?.error || `Metered API HTTP ${response.status}`;
      const err = new Error(message);
      err.status = response.status;
      err.body = body;
      throw err;
    }
    return body;
  } finally {
    clearTimeout(timer);
  }
}

function extractIceServers(payload) {
  const candidates = [
    Array.isArray(payload) ? payload : null,
    Array.isArray(payload?.iceServers) ? payload.iceServers : null,
    Array.isArray(payload?.data) ? payload.data : null,
    Array.isArray(payload?.data?.iceServers) ? payload.data.iceServers : null,
  ].filter(Boolean);
  const list = candidates[0] || [];
  return list.filter(item => item && item.urls);
}

async function createMeteredRoomCredential(roomId) {
  const label = `room-${roomId}-${Date.now()}`;
  const created = await meteredRequest(`/api/v1/turn/credential?secretKey=${encodeURIComponent(METERED_SECRET_KEY)}`, {
    method: 'POST',
    body: JSON.stringify({ expiryInSeconds: TURN_CREDENTIAL_TTL, label }),
  });
  if (!created?.username || !created?.password || !created?.apiKey) throw new Error('METERED_INVALID_CREATE_RESPONSE');

  try {
    const icePayload = await meteredRequest(`/api/v1/turn/credentials?apiKey=${encodeURIComponent(created.apiKey)}&region=${encodeURIComponent(METERED_REGION)}`, { method: 'GET' });
    const iceServers = extractIceServers(icePayload);
    if (!iceServers.length) throw new Error('METERED_NO_ICE_SERVERS');

    return {
      username: String(created.username),
      password: String(created.password),
      apiKey: String(created.apiKey),
      iceServers,
      createdAt: Date.now(),
      expiresAt: Date.now() + TURN_CREDENTIAL_TTL * 1000,
      label,
    };
  } catch (error) {
    await disableMeteredCredential({ username: created.username });
    throw error;
  }
}

async function disableMeteredCredential(session) {
  if (!session?.username || !meteredConfigured()) return;
  try {
    await meteredRequest(`/api/v1/turn/credential/disable?secretKey=${encodeURIComponent(METERED_SECRET_KEY)}`, {
      method: 'POST',
      body: JSON.stringify({ username: session.username }),
    });
    console.log('[TURN] disabled credential', session.username);
  } catch (error) {
    console.warn('[TURN] disable failed:', error.message);
  }
}

function scheduleTurnCleanup(roomId) {
  setTimeout(async () => {
    const users = roomUsers.get(roomId);
    if (users && users.size) return;
    const session = roomTurnSessions.get(roomId);
    if (!session) return;
    if (session.status === 'creating') {
      session.cancelAfterCreate = true;
      return;
    }
    await disableMeteredCredential(session);
    roomTurnSessions.delete(roomId);
  }, TURN_EMPTY_ROOM_DISABLE_DELAY_MS);
}

function prepareRoomTurn(roomId) {
  if (!meteredConfigured()) return Promise.resolve({ ok: false, status: 'disabled', iceServers: buildFallbackIceServers() });
  const existing = roomTurnSessions.get(roomId);
  if (existing?.promise) return existing.promise;
  if (existing?.status === 'ready') return Promise.resolve({ ok: true, status: 'ready', iceServers: existing.iceServers, expiresAt: existing.expiresAt });

  const state = existing || { status: 'creating', promise: null, cancelAfterCreate: false };
  state.status = 'creating';
  state.promise = (async () => {
    try {
      const session = await createMeteredRoomCredential(roomId);
      session.status = 'ready';
      session.promise = null;
      roomTurnSessions.set(roomId, session);
      console.log('[TURN] created session credential', roomId, session.username);
      const publicState = { ok: true, status: 'ready', iceServers: session.iceServers, expiresAt: session.expiresAt };
      const users = roomUsers.get(roomId);
      if (users?.size) io.to(roomId).emit('turn-ready', publicState);
      if (state.cancelAfterCreate && !users?.size) scheduleTurnCleanup(roomId);
      return publicState;
    } catch (error) {
      roomTurnSessions.delete(roomId);
      console.error('[TURN] prepare failed:', error.message);
      const publicState = { ok: false, status: 'error', error: 'TURN_PREPARE_FAILED', iceServers: buildFallbackIceServers() };
      const users = roomUsers.get(roomId);
      if (users?.size) io.to(roomId).emit('turn-error', publicState);
      return publicState;
    }
  })();
  roomTurnSessions.set(roomId, state);
  return state.promise;
}

function getRoomTurnPublicState(roomId) {
  const session = roomTurnSessions.get(roomId);
  if (session?.status === 'ready') return { ok: true, status: 'ready', iceServers: session.iceServers, expiresAt: session.expiresAt };
  if (meteredConfigured()) return { ok: true, status: session?.status || 'preparing', iceServers: publicIceServersOnly() };
  return { ok: false, status: 'disabled', iceServers: buildFallbackIceServers() };
}

function normalizeRoomId(value) {
  return String(value || '').trim().replace(/[^a-zA-Z0-9_-]/g, '').slice(0, 64);
}
function sanitizeName(value) {
  return String(value || 'Guest').replace(/[<>]/g, '').trim().slice(0, 40) || 'Guest';
}
function sanitizeText(value) {
  return String(value || '').replace(/[\u0000-\u0008\u000B\u000C\u000E-\u001F]/g, '').trim().slice(0, 2000);
}
function makeRoomId() { return crypto.randomBytes(4).toString('hex').toUpperCase(); }
function getRoomUsers(roomId) { const users = roomUsers.get(roomId); return users ? [...users.values()] : []; }
function getHistory(roomId) { return roomMessages.get(roomId) || []; }
function pushHistory(roomId, message) {
  const history = roomMessages.get(roomId) || [];
  history.push(message);
  while (history.length > MAX_CHAT_HISTORY) history.shift();
  roomMessages.set(roomId, history);
  persistChatHistory();
}
function publicMessage(socket, extra = {}) {
  return {
    id: crypto.randomBytes(8).toString('hex'),
    roomId: socketRoom.get(socket.id),
    senderSocketId: socket.id,
    senderName: socket.data.displayName || 'Guest',
    timestamp: Date.now(),
    ...extra,
  };
}

function leaveRoom(socket, reason = 'left') {
  const roomId = socketRoom.get(socket.id);
  if (!roomId) return;
  const users = roomUsers.get(roomId);
  if (users) {
    users.delete(socket.id);
    if (users.size === 0) {
      roomUsers.delete(roomId);
      // Chat history remains persisted so the room can be reopened later.
      persistChatHistory();
      scheduleTurnCleanup(roomId);
    }
  }
  socketRoom.delete(socket.id);
  socket.leave(roomId);
  socket.to(roomId).emit('peer-left', { reason });
}

app.use(express.json({ limit: '1mb' }));

app.get('/api/config', (_req, res) => {
  const iceServers = publicIceServersOnly();
  const hasDynamicTurn = meteredConfigured();
  const hasStaticFallback = Boolean(process.env.TURN_URLS && process.env.TURN_USERNAME && process.env.TURN_CREDENTIAL);
  res.json({
    appName: APP_NAME,
    maxRoomUsers: MAX_ROOM_USERS,
    maxFileSize: MAX_FILE_SIZE,
    iceServers,
    hasTurn: hasDynamicTurn || hasStaticFallback,
    turnMode: hasDynamicTurn ? 'metered-dynamic' : (hasStaticFallback ? 'static-fallback' : 'stun-only'),
    turnRegion: METERED_REGION,
    serverTime: new Date().toISOString(),
  });
});

app.get('/health', (_req, res) => {
  let users = 0;
  for (const room of roomUsers.values()) users += room.size;
  res.json({ ok: true, rooms: roomUsers.size, users, maxFileSize: MAX_FILE_SIZE });
});

app.get('/api/push/public-key', (_req, res) => {
  res.json({ ok: true, publicKey: pushVapid.publicKey });
});

app.post('/api/push/subscribe', express.json({ limit: '32kb' }), (req, res) => {
  try {
    const roomId = normalizeRoomId(req.body?.roomId);
    const socketId = String(req.body?.socketId || '');
    const subscription = req.body?.subscription;
    const displayName = sanitizeName(req.body?.displayName);
    const lang = ['ar','en','cs'].includes(req.body?.lang) ? req.body.lang : 'en';
    if (!roomId || !socketId || socketRoom.get(socketId) !== roomId || !subscription?.endpoint) {
      return res.status(403).json({ ok: false, error: 'NOT_IN_ROOM' });
    }
    const now = Date.now();
    const existing = pushSubscriptions.find(s => s.endpoint === subscription.endpoint);
    const record = { roomId, socketId, displayName, lang, subscription, createdAt: existing?.createdAt || now, lastSeenAt: now, visible: true };
    if (existing) Object.assign(existing, record); else pushSubscriptions.push(record);
    persistPushSubscriptions();
    res.json({ ok: true });
  } catch (error) { console.error('[PUSH SUBSCRIBE]', error); res.status(400).json({ ok: false, error: 'SUBSCRIBE_FAILED' }); }
});

app.post('/api/push/unsubscribe', express.json({ limit: '32kb' }), (req, res) => {
  const endpoint = String(req.body?.endpoint || '');
  if (!endpoint) return res.json({ ok: true });
  const index = pushSubscriptions.findIndex(s => s.endpoint === endpoint);
  if (index >= 0) { pushSubscriptions.splice(index, 1); persistPushSubscriptions(); }
  res.json({ ok: true });
});

app.post('/api/chat-history/download', express.json({ limit: '64kb' }), (req, res) => {
  const roomId = normalizeRoomId(req.body?.roomId);
  const socketId = String(req.body?.socketId || '');
  if (!roomId || !socketId || socketRoom.get(socketId) !== roomId) return res.status(403).json({ ok: false, error: 'NOT_IN_ROOM' });
  res.json({ ok: true, roomId, history: getHistory(roomId) });
});

app.use('/uploads', express.static(UPLOAD_DIR, {
  fallthrough: false,
  setHeaders: (res) => { res.setHeader('X-Content-Type-Options', 'nosniff'); }
}));
app.use(express.static(path.join(__dirname, 'public')));

async function notifyRoomPush(roomId, message) {
  const targets = pushSubscriptions.filter(s => s.roomId === roomId && s.socketId !== message.senderSocketId && s.visible !== true);
  if (!targets.length) return;
  const payload = JSON.stringify({
    type: 'chat-message', roomId, messageId: message.id,
    senderName: message.senderName || 'Guest',
    title: 'New message',
    preview: message.kind === 'file' ? (message.file?.type?.startsWith('image/') ? '🖼️ Image' : message.file?.type?.startsWith('audio/') ? '🎙️ Voice message' : `📎 ${message.file?.name || 'File'}`) : String(message.text || '').slice(0, 120),
    url: `/?room=${encodeURIComponent(roomId)}`
  });
  await Promise.allSettled(targets.map(async item => {
    try {
      const localized = JSON.parse(payload);
      localized.title = item.lang === 'ar' ? 'رسالة جديدة' : item.lang === 'cs' ? 'Nová zpráva' : 'New message';
      await webpush.sendNotification(item.subscription, JSON.stringify(localized), { TTL: 300, urgency: 'high' });
    }
    catch (error) {
      if (error?.statusCode === 404 || error?.statusCode === 410) {
        const i = pushSubscriptions.indexOf(item); if (i >= 0) pushSubscriptions.splice(i, 1);
      }
      console.warn('[PUSH]', error?.statusCode || '', error?.message || error);
    }
  }));
  persistPushSubscriptions();
}

app.post('/api/upload', upload.single('file'), (req, res) => {
  try {
    const roomId = normalizeRoomId(req.headers['x-room-id']);
    const socketId = String(req.headers['x-socket-id'] || '');
    if (!roomId || !socketId || socketRoom.get(socketId) !== roomId) {
      if (req.file) fs.rmSync(req.file.path, { force: true });
      return res.status(403).json({ ok: false, error: 'NOT_IN_ROOM' });
    }
    if (!req.file) return res.status(400).json({ ok: false, error: 'FILE_REQUIRED' });

    const message = publicMessage({ id: socketId, data: { displayName: sanitizeName(roomUsers.get(roomId)?.get(socketId)?.displayName) } }, {
      kind: 'file',
      text: sanitizeText(req.body?.caption),
      file: {
        url: `/uploads/${encodeURIComponent(req.file.filename)}`,
        name: String(req.file.originalname || req.file.filename).slice(0, 180),
        size: req.file.size,
        type: req.file.mimetype || 'application/octet-stream',
      },
    });
    pushHistory(roomId, message);
    io.to(roomId).emit('chat-message', message);
    notifyRoomPush(roomId, message).catch(() => {});
    res.json({ ok: true, message });
  } catch (error) {
    if (req.file) fs.rmSync(req.file.path, { force: true });
    console.error('[UPLOAD]', error);
    res.status(500).json({ ok: false, error: 'UPLOAD_FAILED' });
  }
});

// Friendly JSON errors for multer.
app.use((error, _req, res, _next) => {
  if (error?.code === 'LIMIT_FILE_SIZE') return res.status(413).json({ ok: false, error: 'FILE_TOO_LARGE' });
  if (error?.message === 'FILE_TYPE_NOT_ALLOWED') return res.status(415).json({ ok: false, error: 'FILE_TYPE_NOT_ALLOWED' });
  if (error) return res.status(400).json({ ok: false, error: 'UPLOAD_FAILED' });
});

io.on('connection', socket => {
  socket.emit('server-info', { appName: APP_NAME, socketId: socket.id });

  socket.on('create-room', (_payload, callback) => {
    let roomId = makeRoomId();
    while (roomUsers.has(roomId)) roomId = makeRoomId();
    prepareRoomTurn(roomId);
    callback?.({ ok: true, roomId, turn: getRoomTurnPublicState(roomId) });
  });

  socket.on('join-room', (payload, callback) => {
    const roomId = normalizeRoomId(payload?.roomId);
    const displayName = sanitizeName(payload?.displayName);
    if (!roomId) return callback?.({ ok: false, error: 'ROOM_REQUIRED' });
    if (socketRoom.has(socket.id)) leaveRoom(socket, 'rejoin');

    let users = roomUsers.get(roomId);
    if (!users) { users = new Map(); roomUsers.set(roomId, users); }
    if (users.size >= MAX_ROOM_USERS) return callback?.({ ok: false, error: 'ROOM_FULL' });

    const isCaller = users.size === 0;
    users.set(socket.id, { socketId: socket.id, displayName, joinedAt: Date.now() });
    socketRoom.set(socket.id, roomId);
    socket.data.displayName = displayName;
    socket.data.roomId = roomId;
    socket.join(roomId);

    if (!roomTurnSessions.has(roomId)) prepareRoomTurn(roomId);
    const turn = getRoomTurnPublicState(roomId);
    const peer = [...users.values()].find(u => u.socketId !== socket.id) || null;
    callback?.({ ok: true, roomId, isCaller, count: users.size, hasPeer: Boolean(peer), peer: peer ? { socketId: peer.socketId, displayName: peer.displayName } : null, history: getHistory(roomId), turn });

    if (peer) {
      socket.to(roomId).emit('peer-joined', { socketId: socket.id, displayName });
      socket.emit('peer-ready', { socketId: peer.socketId, displayName: peer.displayName });
    }
  });

  const relayEvents = ['offer', 'answer', 'ice-candidate', 'renegotiate-offer', 'renegotiate-answer', 'call-state'];
  for (const eventName of relayEvents) {
    socket.on(eventName, data => {
      const roomId = socketRoom.get(socket.id);
      if (!roomId) return;
      socket.to(roomId).emit(eventName, { ...(data || {}), senderSocketId: socket.id });
    });
  }

  socket.on('chat-message', payload => {
    const roomId = socketRoom.get(socket.id);
    const text = sanitizeText(payload?.text);
    if (!roomId || !text) return;
    const message = publicMessage(socket, { kind: 'text', text });
    pushHistory(roomId, message);
    io.to(roomId).emit('chat-message', message);
    notifyRoomPush(roomId, message).catch(() => {});
  });

  socket.on('chat-typing', payload => {
    const roomId = socketRoom.get(socket.id);
    if (!roomId) return;
    socket.to(roomId).emit('chat-typing', { isTyping: Boolean(payload?.isTyping), senderName: socket.data.displayName || 'Guest' });
  });

  socket.on('push-visibility', payload => {
    const endpoint = String(payload?.endpoint || '');
    const record = pushSubscriptions.find(s => s.socketId === socket.id && (!endpoint || s.endpoint === endpoint));
    if (record) { record.visible = Boolean(payload?.visible); record.lastSeenAt = Date.now(); persistPushSubscriptions(); }
  });

  socket.on('push-subscription-socket', payload => {
    const endpoint = String(payload?.endpoint || '');
    const record = pushSubscriptions.find(s => s.endpoint === endpoint);
    if (record) { record.socketId = socket.id; record.roomId = socketRoom.get(socket.id) || record.roomId; record.displayName = socket.data.displayName || record.displayName; record.visible = Boolean(payload?.visible ?? true); record.lastSeenAt = Date.now(); persistPushSubscriptions(); }
  });

  socket.on('request-reconnect', () => {
    const roomId = socketRoom.get(socket.id);
    if (roomId) socket.to(roomId).emit('peer-reconnect-request');
  });

  socket.on('hangup', () => {
    const roomId = socketRoom.get(socket.id);
    if (roomId) socket.to(roomId).emit('remote-hangup');
  });

  socket.on('disconnect', reason => {
    for (const record of pushSubscriptions) if (record.socketId === socket.id) { record.visible = false; record.lastSeenAt = Date.now(); }
    persistPushSubscriptions();
    leaveRoom(socket, reason || 'disconnect');
  });
});

server.listen(PORT, '0.0.0.0', () => console.log(`${APP_NAME} running on http://localhost:${PORT}`));
