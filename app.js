const express = require('express');
const cors = require("cors");
const mongoose = require('mongoose');
const socketIO = require("socket.io");

require('dotenv').config();

const app = express();

// ── Port: Suga (and most PaaS) inject process.env.PORT.
//    Fall back to 80 to match Suga's default public port mapping.
const port = process.env.PORT || 80;

// ── Bind to 0.0.0.0 so the container is reachable from Suga's reverse proxy.
//    Using 'localhost' would only accept connections from inside the container.
const server = app.listen(port, '0.0.0.0', () => {
    console.log(`API listening on port ${port}.`);
});

server.on('error', (err) => {
    console.error('Server error:', err);
    process.exit(1);
});

// ── Socket.IO with correct CORS options (v4 syntax) ─────────────
const io = socketIO(server, {
    cors: {
        origin: '*',
        methods: ['GET', 'POST'],
        credentials: false
    },
    transports: ['polling', 'websocket']
});

// ── MongoDB is OPTIONAL for Watch Together. ─────────────────────
// The socket.io relay uses an in-memory `users` object and never
// touches the DB. We only attempt a connection if DB_URI is set,
// and we swallow the error so the server stays alive regardless.
const mongoURI = process.env.DB_URI;

if (mongoURI && typeof mongoURI === 'string' && mongoURI.trim().length > 0) {
    mongoose.connect(mongoURI, { useNewUrlParser: true, useUnifiedTopology: true })
        .then(() => console.log('MongoDB connected.'))
        .catch(err => {
            console.warn('⚠ MongoDB connection failed (continuing without DB):', err.message);
        });
} else {
    console.log('ℹ DB_URI not set — running without MongoDB (Watch Together still works).');
}

app.set('view engine', 'ejs');
app.use('/public', express.static('public'));

app.use(express.json());
app.use(express.urlencoded({ extended: false }));
app.use(cors());

app.use('/room', require('./routes/room'));

// ── Simple health check for Suga / uptime monitors ──────────────
app.get('/', (req, res) => {
    res.send('Local Party API running');
});

// ── In-memory user store (per socket.id) ────────────────────────
const users = {};

io.on('connection', (socket) => {

    socket.on('new-user-joined', (data) => {
        try {
            users[socket.id] = {
                name: data.name,
                roomCode: data.roomCode,
                pfp: data.pfp
            };
            socket.join(data.roomCode);

            // Count members in the same room
            let newUsers = {};
            for (const [key, value] of Object.entries(users)) {
                if (value.roomCode === data.roomCode) {
                    newUsers[key] = users[key];
                }
            }
            const memberCount = Object.keys(newUsers).length;

            socket.broadcast.emit('user-joined', {
                name: data.name,
                roomCode: data.roomCode,
                pfp: data.pfp,
                members: memberCount
            });

            // Send the joining user their own member info
            setTimeout(() => {
                socket.emit('updateMemberInfo', {
                    roomCode: data.roomCode,
                    members: memberCount
                });
            }, 500);
        } catch (err) {
            console.error('new-user-joined error:', err);
        }
    });

    socket.on('send', (message) => {
        try {
            const user = users[socket.id];
            if (!user) return;
            socket.to(user.roomCode).emit('receive', {
                message: message,
                name: user.name,
                pfp: user.pfp
            });
        } catch (err) {
            console.error('send error:', err);
        }
    });

    socket.on('disconnectUser', (name) => {
        try {
            const user = users[socket.id];
            if (!user) return;

            let newUsers = {};
            for (const [key, value] of Object.entries(users)) {
                if (value.roomCode === user.roomCode) {
                    newUsers[key] = users[key];
                }
            }

            socket.to(user.roomCode).emit('left', {
                name: user.name,
                pfp: user.pfp,
                members: Math.max(0, Object.keys(newUsers).length - 1)
            });

            delete users[socket.id];
            socket.disconnect(true);
        } catch (err) {
            console.error('disconnectUser error:', err);
        }
    });

    socket.on('playerControl', (data) => {
        try {
            const user = users[socket.id];
            if (!user) return;
            socket.to(data.roomCode).emit('playerControlUpdate', {
                message: data.message,
                context: data.context,
                username: user.name
            });
        } catch (err) {
            console.error('playerControl error:', err);
        }
    });

    socket.on('disconnect', (reason) => {
        try {
            const user = users[socket.id];
            if (!user) return;

            let newUsers = {};
            for (const [key, value] of Object.entries(users)) {
                if (value.roomCode === user.roomCode) {
                    newUsers[key] = users[key];
                }
            }

            socket.to(user.roomCode).emit('leftdefault', {
                name: user.name,
                pfp: user.pfp,
                members: Math.max(0, Object.keys(newUsers).length - 1)
            });

            delete users[socket.id];
        } catch (error) {
            console.log('disconnect error:', error);
        }
    });
});