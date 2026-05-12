// dashboard/server.js
// Dashboard server: HTTP + WebSocket bridge dari MQTT ke browser

const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const mqtt = require('mqtt');
const path = require('path');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

const PORT = 3000;
const BROKER_URL = 'mqtt://localhost:1883';

// State cache untuk dashboard
const state = {
  prices: {},
  alerts: [],
  market: null,
  trending: null,
  publishers: {},
  mqttFeatures: [],
};

// Connect ke MQTT broker sebagai subscriber dashboard
const mqttClient = mqtt.connect(BROKER_URL, {
  clientId: 'dashboard-server',
  protocolVersion: 5,
  clean: false,
  will: {
    topic: 'crypto/status/dashboard',
    payload: JSON.stringify({ status: 'offline', publisher: 'dashboard', timestamp: new Date().toISOString() }),
    qos: 1,
    retain: true,
  },
  properties: { receiveMaximum: 50 },
});

mqttClient.on('connect', () => {
  console.log('✅ Dashboard connected to MQTT broker');

  mqttClient.publish(
    'crypto/status/dashboard',
    JSON.stringify({ status: 'online', publisher: 'dashboard', timestamp: new Date().toISOString() }),
    { qos: 1, retain: true }
  );

  // Subscribe ke semua topic crypto (Fitur 2: Wildcard #)
  mqttClient.subscribe('crypto/#', { qos: 1 });
});

mqttClient.on('message', (topic, message, packet) => {
  try {
    const data = JSON.parse(message.toString());

    let event = null;

    if (topic.startsWith('crypto/price/')) {
      const coin = topic.split('/')[2];
      state.prices[coin] = { ...data, retained: packet.retain };
      event = { type: 'price_update', coin, data, retained: packet.retain, qos: packet.qos };
      logFeature('QoS 1', `Harga ${data.symbol} diterima dengan jaminan`);
      if (packet.retain) {
        logFeature('Retain', `📌 ${data.symbol} diterima dari CACHE broker — subscriber baru dapat data tanpa menunggu publish baru`);
      } else {
        logFeature('Retain', `${data.symbol} dipublish retain:true → broker simpan pesan ini untuk subscriber yang baru connect`);
      }

    } else if (topic === 'crypto/alerts') {
      state.alerts.unshift({ ...data, id: Date.now() });
      if (state.alerts.length > 20) state.alerts = state.alerts.slice(0, 20);
      event = { type: 'alert', data, qos: packet.qos };
      logFeature('QoS 2', `Alert ${data.symbol} dikirim Exactly Once`);
      if (packet.properties?.messageExpiryInterval) {
        logFeature('Message Expiry', `Alert expires dalam ${packet.properties.messageExpiryInterval}s`);
      }

    } else if (topic === 'crypto/market/global') {
      state.market = data;
      event = { type: 'market_update', data };

    } else if (topic === 'crypto/market/trending') {
      state.trending = data;
      event = { type: 'trending_update', data };

    } else if (topic.startsWith('crypto/status/')) {
      const publisher = topic.split('/')[2];
      state.publishers[publisher] = { ...data, retained: packet.retain };
      event = { type: 'status_update', publisher, data };
      if (data.status === 'offline') logFeature('LWT (Last Will Testament)', `${publisher} mati mendadak → broker otomatis publish "offline"`);
      if (packet.retain) logFeature('Retain', `📌 Status ${publisher} diterima dari cache broker (${data.status})`);
    
    } else if (topic.startsWith('crypto/response/dashboard-')) {
      // Tangkap response dari alert publisher
      const responseData = data;
      logFeature('Request-Response', `Dashboard menerima response: Status ${responseData.status} (Threshold: ${responseData.threshold}%)`);
      event = { type: 'alert_response', data: responseData };
    }

    if (packet.properties?.userProperties) {
      logFeature('User Properties', `Metadata: ${JSON.stringify(packet.properties.userProperties)}`);
    }

    if (event) broadcast(event);

  } catch (e) { /* skip non-json */ }
});

function logFeature(feature, detail) {
  const entry = { feature, detail, timestamp: new Date().toISOString() };
  state.mqttFeatures.unshift(entry);
  if (state.mqttFeatures.length > 50) state.mqttFeatures = state.mqttFeatures.slice(0, 50);
  broadcast({ type: 'feature_log', data: entry });
}

function broadcast(data) {
  const msg = JSON.stringify(data);
  wss.clients.forEach(ws => {
    if (ws.readyState === WebSocket.OPEN) ws.send(msg);
  });
}

// WebSocket connection handler
wss.on('connection', (ws) => {
  console.log('🖥️  Dashboard client connected');
  // Kirim state awal ke browser yang baru connect
  ws.send(JSON.stringify({ type: 'initial_state', data: state }));

  ws.on('message', (msg) => {
    try {
      const cmd = JSON.parse(msg.toString());
      if (cmd.type === 'request_alert_status') {
        // Fitur 8: Forward request ke alert publisher via MQTT
        const requestId = `req-${Date.now()}`;
        const responseTopic = `crypto/response/dashboard-${requestId}`;

        mqttClient.subscribe(responseTopic, { qos: 1 }, () => {
          mqttClient.publish('crypto/request/alert', JSON.stringify({ request_id: requestId, requester: 'dashboard' }), {
            qos: 1,
            properties: {
              responseTopic,
              correlationData: Buffer.from(requestId),
              messageExpiryInterval: 15,
            },
          });
          logFeature('Request-Response', `Dashboard mengirim request ke alert publisher`);
        });

        // Unsubscribe setelah dapat response
        setTimeout(() => mqttClient.unsubscribe(responseTopic), 20000);
      }
    } catch (e) { }
  });
});

// Serve static dashboard
app.use(express.static(path.join(__dirname, 'public')));

server.listen(PORT, () => {
  console.log(`🌐 Dashboard running at http://localhost:${PORT}`);
});
