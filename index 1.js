import express from 'express';
import http from 'http';
import cors from 'cors';
import helmet from 'helmet';
import rateLimit from 'express-rate-limit';
import { createClient } from 'redis';
import { createAdapter } from '@socket.io/redis-adapter';
import dotenv from 'dotenv';
import { Server } from 'socket.io';
import { MongoClient } from 'mongodb';

dotenv.config();

const PORT = Number(process.env.PORT || 4000);
const CLIENT_ORIGIN =
  process.env.CLIENT_ORIGIN || 'http://localhost:5173';

// Phase 2 — Day 2: Real-time presence settings
const AWAY_AFTER_MS = 60_000;
const PRESENCE_REFRESH_MS = 15_000;

// Phase 2 — Day 4: Proximity settings
const PROXIMITY_RADIUS = 100;

const app = express();

app.disable('x-powered-by');
app.use(helmet({ crossOriginEmbedderPolicy: false }));
app.use(cors({ origin: CLIENT_ORIGIN }));

app.use(
  '/api/',
  rateLimit({
    windowMs: 60_000,
    limit: 120,
    standardHeaders: 'draft-7',
    legacyHeaders: false,
  })
);

app.use(express.json({ limit: '1mb' }));

const httpServer = http.createServer(app);

const io = new Server(httpServer, {
  cors: {
    origin: CLIENT_ORIGIN,
    methods: ['GET', 'POST'],
  },
});

// --------------------------------------------------
// MONGODB
// --------------------------------------------------

let mongoClient;
let positionsCollection;
let roomsCollection;

if (process.env.MONGODB_URI) {
  try {
    mongoClient = new MongoClient(process.env.MONGODB_URI);
    await mongoClient.connect();

    const db = mongoClient.db('proxispeak');

    positionsCollection = db.collection('positions');
    roomsCollection = db.collection('rooms');

    await positionsCollection.createIndex(
      { roomId: 1, socketId: 1 },
      { unique: true }
    );

    await roomsCollection.createIndex(
      { roomId: 1 },
      { unique: true }
    );

    // Pixel coordinates are not geographic coordinates.
    // Do not create a 2dsphere index for x/y positions.

    console.log('MongoDB connected; indexes ready');
  } catch (error) {
    console.error(
      'MongoDB setup failed; continuing in-memory:',
      error.message
    );

    positionsCollection = null;
    roomsCollection = null;
  }
}

// --------------------------------------------------
// USERS AND VALIDATION
// --------------------------------------------------

const users = new Map();
let redisClients = null;

const roomUsers = (roomId) =>
  [...users.values()].filter((user) => user.roomId === roomId);

const broadcastRoomUsers = (roomId) => {
  io.to(roomId).emit('users:update', roomUsers(roomId));
};

// Validate coordinates and keep avatars within office boundaries.
const validCoord = (value, max) => {
  if (typeof value !== 'number' || !Number.isFinite(value)) {
    return null;
  }

  return Math.max(24, Math.min(max, value));
};

// --------------------------------------------------
// PHASE 2 — DAY 4: PROXIMITY HELPERS
// --------------------------------------------------

// Calculate the straight-line distance between two avatars.
const getDistance = (userA, userB) => {
  return Math.hypot(
    userA.x - userB.x,
    userA.y - userB.y
  );
};

// Find users within 100 pixels of the given avatar.
// Only users in the same room are considered.
// The current in-memory positions are authoritative.
const findNearbyUsers = (
  roomId,
  x,
  y,
  excludeSocketId
) => {
  return roomUsers(roomId).filter((user) => {
    if (user.id === excludeSocketId) {
      return false;
    }

    const distance = Math.hypot(
      user.x - x,
      user.y - y
    );

    return distance <= PROXIMITY_RADIUS;
  });
};

// Send the current nearby participant IDs to one socket.
const updateProximityForSocket = (socket, user) => {
  const nearbyUsers = findNearbyUsers(
    user.roomId,
    user.x,
    user.y,
    socket.id
  );

  socket.emit('proximity:update', {
    radius: PROXIMITY_RADIUS,
    nearbyUserIds: nearbyUsers.map(
      (nearbyUser) => nearbyUser.id
    ),
  });
};

// Refresh proximity lists for all participants in one room.
const broadcastRoomProximity = (roomId) => {
  const participants = roomUsers(roomId);

  for (const user of participants) {
    const participantSocket = io.sockets.sockets.get(user.id);

    if (participantSocket) {
      updateProximityForSocket(participantSocket, user);
    }
  }
};

// --------------------------------------------------
// REDIS ADAPTER (OPTIONAL)
// --------------------------------------------------

if (process.env.REDIS_URL) {
  try {
    const pubClient = createClient({
      url: process.env.REDIS_URL,
    });

    const subClient = pubClient.duplicate();

    pubClient.on('error', (error) =>
      console.error('Redis publisher error:', error.message)
    );

    subClient.on('error', (error) =>
      console.error('Redis subscriber error:', error.message)
    );

    await Promise.all([
      pubClient.connect(),
      subClient.connect(),
    ]);

    io.adapter(createAdapter(pubClient, subClient));

    redisClients = [pubClient, subClient];

    console.log('Socket.IO Redis adapter enabled');
  } catch (error) {
    console.error(
      'Redis adapter unavailable; using single-instance Socket.IO:',
      error.message
    );
  }
}

// --------------------------------------------------
// HTTP API ROUTES
// --------------------------------------------------

app.get('/api/health', (_req, res) => {
  res.json({
    ok: true,
    service: 'ProxiSpeak',
    connectedUsers: users.size,
    persistence: Boolean(positionsCollection),
    redisAdapter: Boolean(redisClients),
  });
});

app.get('/api/ready', (_req, res) => {
  if (process.env.MONGODB_URI && !positionsCollection) {
    return res.status(503).json({
      ready: false,
      reason: 'MongoDB unavailable',
    });
  }

  res.json({ ready: true });
});

app.get('/api/users', (req, res) => {
  const roomId = String(
    req.query.roomId || 'main-office'
  ).slice(0, 40);

  res.json(roomUsers(roomId));
});

app.get('/api/rooms/:roomId', async (req, res) => {
  try {
    const roomId = String(req.params.roomId).slice(0, 40);

    const saved = roomsCollection
      ? await roomsCollection.findOne({ roomId })
      : null;

    res.json(
      saved || {
        roomId,
        layout: 'default',
        persisted: false,
      }
    );
  } catch (error) {
    console.error('Room layout load failed:', error.message);

    res.status(500).json({
      error: 'Could not load room layout',
    });
  }
});

app.put('/api/rooms/:roomId', async (req, res) => {
  if (!roomsCollection) {
    return res.status(503).json({
      error: 'Configure MONGODB_URI to persist room layouts',
    });
  }

  const roomId = String(req.params.roomId).slice(0, 40);
  const layout = req.body?.layout;

  if (!layout || typeof layout !== 'object' || Array.isArray(layout)) {
    return res.status(400).json({
      error: 'layout object required',
    });
  }

  try {
    await roomsCollection.updateOne(
      { roomId },
      {
        $set: {
          roomId,
          layout,
          updatedAt: new Date(),
        },
      },
      { upsert: true }
    );

    res.json({ ok: true, roomId });
  } catch (error) {
    console.error('Room layout save failed:', error.message);

    res.status(500).json({
      error: 'Could not save room layout',
    });
  }
});

// --------------------------------------------------
// SOCKET.IO EVENTS
// --------------------------------------------------

io.on('connection', (socket) => {
  // ------------------------------------------------
  // JOIN OFFICE
  // ------------------------------------------------

  socket.on('office:join', (payload = {}) => {
    // Prevent repeated joins and accidental room switching.
    if (users.has(socket.id)) {
      socket.emit('office:join:error', {
        message: 'You have already joined a room.',
      });
      return;
    }

    if (
      !payload ||
      typeof payload !== 'object' ||
      Array.isArray(payload)
    ) {
      socket.emit('office:join:error', {
        message: 'Invalid join request.',
      });
      return;
    }

    const { name, roomId } = payload;

    const safeName =
      String(name || 'Guest').trim().slice(0, 24) || 'Guest';

    const safeRoom =
      String(roomId || 'main-office').trim().slice(0, 40) ||
      'main-office';

    const user = {
      id: socket.id,
      name: safeName,
      roomId: safeRoom,
      x: 120 + Math.random() * 480,
      y: 100 + Math.random() * 280,
      joinedAt: new Date().toISOString(),

      // Existing presence fields
      status: 'online',
      manualStatus: 'auto',
      lastActivityAt: Date.now(),
    };

    socket.data.roomId = safeRoom;
    socket.join(safeRoom);

    users.set(socket.id, user);

    if (positionsCollection) {
      positionsCollection
        .updateOne(
          { roomId: safeRoom, socketId: socket.id },
          {
            $set: {
              ...user,
              socketId: socket.id,
              updatedAt: new Date(),
            },
          },
          { upsert: true }
        )
        .catch((error) =>
          console.error('Mongo position save failed:', error.message)
        );
    }

    socket.emit('office:joined', {
      selfId: socket.id,
      users: roomUsers(safeRoom),
    });

    socket.to(safeRoom).emit('user:joined', user);

    broadcastRoomUsers(safeRoom);

    // New: update proximity after a user joins.
    broadcastRoomProximity(safeRoom);
  });

  // ------------------------------------------------
  // MANUAL PRESENCE STATUS
  // ------------------------------------------------

  socket.on('presence:update', (payload = {}) => {
    const user = users.get(socket.id);
    if (!user) return;

    if (
      !payload ||
      typeof payload !== 'object' ||
      Array.isArray(payload)
    ) {
      return;
    }

    const requestedStatus = payload.status;

    if (!['online', 'away', 'auto'].includes(requestedStatus)) {
      return;
    }

    user.manualStatus = requestedStatus;

    if (requestedStatus === 'auto') {
      user.status =
        Date.now() - user.lastActivityAt >= AWAY_AFTER_MS
          ? 'away'
          : 'online';
    } else {
      user.status = requestedStatus;
    }

    users.set(socket.id, user);

    broadcastRoomUsers(user.roomId);
  });

  // ------------------------------------------------
  // VALIDATED AVATAR MOVEMENT
  // ------------------------------------------------

  socket.on('avatar:move', (payload) => {
    if (
      !payload ||
      typeof payload !== 'object' ||
      Array.isArray(payload)
    ) {
      return;
    }

    const user = users.get(socket.id);

    // A socket must join an office before moving.
    if (!user) return;

    const nextX = validCoord(payload.x, 776);
    const nextY = validCoord(payload.y, 476);

    // Reject movement if either coordinate is invalid.
    if (nextX === null || nextY === null) {
      socket.emit('avatar:move:error', {
        message: 'Invalid movement coordinates.',
      });
      return;
    }

    user.x = nextX;
    user.y = nextY;

    // Avatar movement counts as activity in automatic mode.
    const previousStatus = user.status;
    user.lastActivityAt = Date.now();

    if (user.manualStatus === 'auto') {
      user.status = 'online';
    }

    users.set(socket.id, user);

    if (previousStatus !== user.status) {
      broadcastRoomUsers(user.roomId);
    }

    if (positionsCollection) {
      positionsCollection
        .updateOne(
          { roomId: user.roomId, socketId: socket.id },
          {
            $set: {
              x: user.x,
              y: user.y,
              updatedAt: new Date(),
            },
          }
        )
        .catch((error) =>
          console.error('Mongo position update failed:', error.message)
        );
    }

    // Preserve existing movement synchronization.
    io.to(user.roomId).emit('avatar:moved', user);

    // New: update proximity for every user in this room.
    broadcastRoomProximity(user.roomId);
  });

  // ------------------------------------------------
  // AUDIO SIGNALING — 100-PIXEL PROXIMITY CHECK
  // ------------------------------------------------

  socket.on('audio:signal', (payload = {}) => {
    if (
      !payload ||
      typeof payload !== 'object' ||
      Array.isArray(payload)
    ) {
      return;
    }

    const { targetId, signal } = payload;

    if (!targetId || !signal) return;

    const sender = users.get(socket.id);
    const target = users.get(targetId);

    if (!sender || !target) return;

    // Prevent signaling between different rooms.
    if (sender.roomId !== target.roomId) return;

    // Ensure all coordinates are valid numbers.
    if (
      ![
        sender.x,
        sender.y,
        target.x,
        target.y,
      ].every(Number.isFinite)
    ) {
      return;
    }

    // Calculate the actual distance between the avatars.
    const distance = getDistance(sender, target);

    // Do not forward WebRTC signaling beyond 100 pixels.
    if (distance > PROXIMITY_RADIUS) {
      return;
    }

    // Only send the signal to the intended target.
    io.to(targetId).emit('audio:signal', {
      fromId: socket.id,
      signal,
    });
  });

  // ------------------------------------------------
  // DISCONNECT AND CLEANUP
  // ------------------------------------------------

  socket.on('disconnect', () => {
    const user = users.get(socket.id);

    if (!user) return;

    users.delete(socket.id);

    if (positionsCollection) {
      positionsCollection
        .deleteOne({
          roomId: user.roomId,
          socketId: socket.id,
        })
        .catch((error) =>
          console.error('Mongo position cleanup failed:', error.message)
        );
    }

    io.to(user.roomId).emit('user:left', {
      id: socket.id,
    });

    broadcastRoomUsers(user.roomId);

    // New: remove the disconnected user from nearby-user lists.
    broadcastRoomProximity(user.roomId);
  });
});

// --------------------------------------------------
// AUTOMATIC PRESENCE REFRESH
// --------------------------------------------------

const presenceTimer = setInterval(() => {
  const now = Date.now();
  const changedRooms = new Set();

  for (const user of users.values()) {
    let nextStatus = user.status;

    if (user.manualStatus === 'away') {
      nextStatus = 'away';
    } else if (user.manualStatus === 'online') {
      nextStatus = 'online';
    } else {
      nextStatus =
        now - user.lastActivityAt >= AWAY_AFTER_MS
          ? 'away'
          : 'online';
    }

    if (nextStatus !== user.status) {
      user.status = nextStatus;
      changedRooms.add(user.roomId);
    }
  }

  for (const roomId of changedRooms) {
    broadcastRoomUsers(roomId);
  }
}, PRESENCE_REFRESH_MS);

presenceTimer.unref();

// --------------------------------------------------
// START SERVER
// --------------------------------------------------

httpServer.listen(PORT, () => {
  console.log(
    `ProxiSpeak server running at http://localhost:${PORT}`
  );

  console.log(
    `Proximity detection enabled: ${PROXIMITY_RADIUS} pixels`
  );
});
