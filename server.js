const express = require('express');
const { createServer } = require('http');
const { Server } = require('ws');

const app = express();
const server = createServer(app);
const wss = new Server({ server });

const rooms = new Map();

wss.on('connection', (ws) => {
    let clientRoom = null;
    let clientId = null;
    let clientName = null;

    ws.on('message', (message) => {
        try {
            const data = JSON.parse(message);
            
            switch(data.type) {
                case 'ping':
                    ws.send(JSON.stringify({ type: 'pong' }));
                    break;

                case 'join':
                    clientRoom = data.room;
                    clientId = data.id;
                    clientName = data.name;
                    
                    if (!rooms.has(clientRoom)) rooms.set(clientRoom, new Map());
                    const room = rooms.get(clientRoom);
                    room.set(clientId, { ws, name: clientName, id: clientId });
                    
                    room.forEach((peer, peerId) => {
                        if (peerId !== clientId && peer.ws.readyState === 1) {
                            peer.ws.send(JSON.stringify({ type: 'peer-joined', id: clientId, name: clientName }));
                            ws.send(JSON.stringify({ type: 'peer-joined', id: peerId, name: peer.name }));
                        }
                    });
                    break;
                    
                case 'signal':
                    if (clientRoom && rooms.has(clientRoom)) {
                        const target = rooms.get(clientRoom).get(data.target);
                        if (target && target.ws.readyState === 1) {
                            target.ws.send(JSON.stringify({
                                type: 'signal',
                                sender: clientId,
                                senderName: clientName,
                                signal: data.signal
                            }));
                        }
                    }
                    break;
                    
                case 'broadcast':
                    if (clientRoom && rooms.has(clientRoom)) {
                        rooms.get(clientRoom).forEach((peer, peerId) => {
                            if (peerId !== clientId && peer.ws.readyState === 1) {
                                peer.ws.send(JSON.stringify({ ...data.payload, sender: clientId, senderName: clientName }));
                            }
                        });
                    }
                    break;
            }
        } catch (e) { console.error(e); }
    });

    ws.on('close', () => {
        if (clientRoom && rooms.has(clientRoom)) {
            const room = rooms.get(clientRoom);
            room.delete(clientId);
            room.forEach((peer) => {
                if (peer.ws.readyState === 1) {
                    peer.ws.send(JSON.stringify({ type: 'peer-left', id: clientId, name: clientName }));
                }
            });
            if (room.size === 0) rooms.delete(clientRoom);
        }
    });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`Server on port ${PORT}`));
