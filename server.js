const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const mineflayer = require('mineflayer');
const { SocksClient } = require('socks');
const dns = require('dns'); // Added for local DNS resolution

const app = express();
const server = http.createServer(app);
const io = new Server(server);

const PORT = process.env.PORT || 3000;
app.use(express.static('public'));

const activeBots = new Map();

function parseProxyString(proxyStr) {
    if (!proxyStr) return null;
    let str = proxyStr.trim();

    let type = 5;
    if (str.startsWith('socks4://')) {
        type = 4;
        str = str.replace('socks4://', '');
    } else if (str.startsWith('socks5://')) {
        type = 5;
        str = str.replace('socks5://', '');
    }

    let host = '';
    let port = 1080;
    let userId = '';
    let password = '';

    if (str.includes('@')) {
        const [auth, hostPart] = str.split('@');
        const [u, p] = auth.split(':');
        userId = u || '';
        password = p || '';
        str = hostPart;
    }

    if (str.startsWith('[')) {
        const closeIdx = str.indexOf(']');
        host = str.substring(1, closeIdx);
        const remaining = str.substring(closeIdx + 2).split(':');
        port = parseInt(remaining[0], 10);
        if (remaining.length >= 3) {
            userId = remaining[1];
            password = remaining.slice(2).join(':');
        }
    } else {
        const parts = str.split(':');
        if (parts.length === 4) {
            host = parts[0];
            port = parseInt(parts[1], 10);
            userId = parts[2];
            password = parts[3];
        } else if (parts.length === 2) {
            host = parts[0];
            port = parseInt(parts[1], 10);
        } else {
            const lastColon = str.lastIndexOf(':');
            port = parseInt(str.substring(lastColon + 1), 10);
            host = str.substring(0, lastColon);
        }
    }

    if (isNaN(port)) throw new Error('Invalid port provided in proxy settings');
    return { host, port, type, userId, password };
}

io.on('connection', (socket) => {
    socket.on('deploy_bots', (data) => {
        const { serverIp, serverPort, botPassword, usernames, proxies, reconnectDelay } = data;
        
        const nameList = usernames && usernames.length > 0 
            ? usernames 
            : [`Bot_${Math.floor(1000 + Math.random() * 9000)}`];

        nameList.forEach((username, index) => {
            const cleanName = username.trim();
            if (!cleanName) return;

            const proxyUrl = proxies && proxies.length > 0 ? proxies[index % proxies.length] : null;
            const delay = parseInt(reconnectDelay) || 5;

            createBot(cleanName, serverIp, parseInt(serverPort), botPassword, proxyUrl, delay, socket);
        });
    });

    socket.on('send_chat', ({ botId, message }) => {
        const bot = activeBots.get(botId);
        if (bot) bot.chat(message);
    });

    socket.on('move_bot', ({ botId, direction, state }) => {
        const bot = activeBots.get(botId);
        if (bot && bot.entity) {
            bot.setControlState(direction, state);
        }
    });

    socket.on('stop_bot', (botId) => {
        const bot = activeBots.get(botId);
        if (bot) {
            bot.quit();
            activeBots.delete(botId);
            socket.emit('bot_status', { botId, status: 'Stopped' });
        }
    });
});

function createBot(username, host, port, password, proxyUrl, reconnectDelay, socket) {
    let parsedProxy = null;

    if (proxyUrl && proxyUrl.trim() !== '') {
        try {
            parsedProxy = parseProxyString(proxyUrl);
            socket.emit('bot_log', { 
                botId: username, 
                text: `Proxy Configuration Loaded: ${parsedProxy.host}:${parsedProxy.port}` 
            });
        } catch (err) {
            socket.emit('bot_log', { botId: username, text: `Proxy Parsing Failed: ${err.message}` });
        }
    }

    const botOptions = {
        host: host,
        port: port,
        username: username,
        auth: 'offline'
    };

    if (parsedProxy) {
        botOptions.connect = (client) => {
            // Emulate Proxifier: Resolve DNS locally before tunneling
            dns.lookup(host, (err, address) => {
                if (err) {
                    socket.emit('bot_log', { botId: username, text: `Local DNS Resolution Failed: ${err.message}` });
                    return;
                }

                socket.emit('bot_log', { botId: username, text: `Resolved server IP to ${address}. Connecting via proxy...` });

                const socksOptions = {
                    proxy: {
                        host: parsedProxy.host,
                        port: parsedProxy.port,
                        type: parsedProxy.type,
                        userId: parsedProxy.userId,
                        password: parsedProxy.password
                    },
                    command: 'connect',
                    destination: {
                        host: address, // Send the raw IP to proxy, avoiding remote DNS failure
                        port: port
                    }
                };

                SocksClient.createConnection(socksOptions, (err, info) => {
                    if (err) {
                        socket.emit('bot_log', { botId: username, text: `Proxy Tunnel Failure: ${err.message}` });
                        return;
                    }
                    client.setSocket(info.socket);
                    client.emit('connect');
                });
            });
        };
    }

    const bot = mineflayer.createBot(botOptions);
    activeBots.set(username, bot);

    let startTime = Date.now();
    let uptimeInterval = setInterval(() => {
        if (activeBots.has(username)) {
            const seconds = Math.floor((Date.now() - startTime) / 1000);
            socket.emit('bot_uptime', { botId: username, uptime: seconds });
        } else {
            clearInterval(uptimeInterval);
        }
    }, 1000);

    bot.on('spawn', () => {
        socket.emit('bot_status', { botId: username, status: 'Online' });
        socket.emit('bot_log', { botId: username, text: 'Successfully joined server. Executing auth commands...' });

        setTimeout(() => {
            bot.chat(`/register ${password} ${password}`);
            bot.chat(`/login ${password}`);
        }, 1500);
    });

    bot.on('message', (jsonMsg) => {
        const text = jsonMsg.toString();
        socket.emit('bot_chat', { botId: username, message: text });
    });

    bot.on('end', (reason) => {
        socket.emit('bot_status', { botId: username, status: 'Disconnected' });
        socket.emit('bot_log', { botId: username, text: `Disconnected: ${reason}. Reconnecting in ${reconnectDelay}s...` });
        clearInterval(uptimeInterval);
        activeBots.delete(username);

        setTimeout(() => {
            createBot(username, host, port, password, proxyUrl, reconnectDelay, socket);
        }, reconnectDelay * 1000);
    });

    bot.on('error', (err) => {
        socket.emit('bot_log', { botId: username, text: `Connection Error: ${err.message}` });
    });
}

server.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
});
      
