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
  // Fitur 2: Wildcard — semua topic yang ditangkap via crypto/#
  wildcardTopics: {},
  // Fitur 2: Wildcard (+) — single-level wildcard matches
  // crypto/price/+ dipakai alertPublisher, crypto/status/+ dipakai alertSubscriber
  wildcardPlusGroups: {
    'crypto/price/+':  { usedBy: 'alertPublisher',  description: 'subscribe harga semua coin sekaligus', matches: {}, count: 0 },
    'crypto/status/+': { usedBy: 'alertSubscriber', description: 'monitor status semua publisher',         matches: {}, count: 0 },
  },
  // Fitur 3: Topic Alias — mapping alias integer → topic asli
  topicAliases: {
    '1': { topic: 'crypto/price/bitcoin',  symbol: 'BTC', lastSeen: null, count: 0 },
    '2': { topic: 'crypto/price/ethereum', symbol: 'ETH', lastSeen: null, count: 0 },
    '3': { topic: 'crypto/price/solana',   symbol: 'SOL', lastSeen: null, count: 0 },
  },
  // Fitur 9: Shared Subscription — load balancing counter
  sharedSub: { dashboardCount: 0, loggerCount: 0, totalAlerts: 0 },
  // Fitur 10: Flow Control — message rate
  flowControl: { receiveMaximum: 50, msgCount: 0, rate: '0.0' },
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
    const _now = new Date().toISOString();

    // Fitur 2: Wildcard — catat setiap topic unik yang masuk via crypto/#
    const isNewTopic = !state.wildcardTopics[topic];
    if (!state.wildcardTopics[topic]) state.wildcardTopics[topic] = { count: 0, lastSeen: null };
    state.wildcardTopics[topic].count++;
    state.wildcardTopics[topic].lastSeen = _now;
    broadcast({ type: 'wildcard_update', data: state.wildcardTopics });
    if (isNewTopic) {
      logFeature('Wildcard (#)', `Topic baru tertangkap: "${topic}" — satu subscription crypto/# mencakup semua sub-topic`);
    }

    // Fitur 2: Wildcard (+) — deteksi match single-level wildcard
    if (topic.startsWith('crypto/price/') && topic.split('/').length === 3) {
      const grp = state.wildcardPlusGroups['crypto/price/+'];
      const isNewMatch = !grp.matches[topic];
      grp.matches[topic] = _now;
      grp.count++;
      if (isNewMatch) {
        logFeature('Wildcard (+)', `alertPublisher subscribe "crypto/price/+" → match: "${topic}" — + hanya satu level, tidak menangkap sub-level`);
      }
      broadcast({ type: 'wildcard_plus_update', data: state.wildcardPlusGroups });
    }
    if (topic.startsWith('crypto/status/') && topic.split('/').length === 3) {
      const grp = state.wildcardPlusGroups['crypto/status/+'];
      const isNewMatch = !grp.matches[topic];
      grp.matches[topic] = _now;
      grp.count++;
      if (isNewMatch) {
        logFeature('Wildcard (+)', `alertSubscriber subscribe "crypto/status/+" → match: "${topic}" — memantau semua publisher status sekaligus`);
      }
      broadcast({ type: 'wildcard_plus_update', data: state.wildcardPlusGroups });
    }

    // Fitur 10: Flow Control — hitung total pesan masuk tiap interval
    state.flowControl.msgCount++;

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

      // Fitur 3: Topic Alias — tandai alias mana yang aktif (dipakai pricePublisher)
      const _aliasEntry = Object.entries(state.topicAliases).find(([, v]) => v.topic === topic);
      if (_aliasEntry) {
        const [_aliasId, _aliasInfo] = _aliasEntry;
        const _prevCount = _aliasInfo.count;
        state.topicAliases[_aliasId].lastSeen = _now;
        state.topicAliases[_aliasId].count++;
        broadcast({ type: 'topic_alias_update', data: state.topicAliases });
        if (_prevCount === 0) {
          logFeature('Topic Alias', `Alias ${_aliasId} → "${topic}" — integer ID mengganti nama topic panjang, hemat bandwidth`);
        } else {
          logFeature('Topic Alias', `Alias ${_aliasId} digunakan ke-${_prevCount + 1} kali — broker decode integer ke full topic name`);
        }
      }

    } else if (topic === 'crypto/alerts') {
      state.alerts.unshift({ ...data, id: Date.now() });
      if (state.alerts.length > 20) state.alerts = state.alerts.slice(0, 20);
      event = { type: 'alert', data, qos: packet.qos };
      logFeature('QoS 2', `Alert ${data.symbol} dikirim Exactly Once`);
      if (packet.properties?.messageExpiryInterval) {
        logFeature('Message Expiry', `Alert expires dalam ${packet.properties.messageExpiryInterval}s`);
      }
      // Fitur 9: track total alert yang publish ke topic ini
      state.sharedSub.totalAlerts++;
      broadcast({ type: 'shared_sub_update', data: state.sharedSub });

    } else if (topic === 'crypto/market/global') {
      state.market = data;
      event = { type: 'market_update', data };
      logFeature('QoS 0', `Market global diterima — fire-and-forget, tanpa acknowledgment dari broker`);

    } else if (topic === 'crypto/market/trending') {
      state.trending = data;
      event = { type: 'trending_update', data };
      logFeature('QoS 0', `Trending coins diterima — QoS 0 cocok untuk data non-kritis yang sering diupdate`);

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

        // crypto/# sudah men-cover responseTopic — tidak perlu subscribe lagi.
        // Langsung publish dan log agar Feature Log selalu muncul.
        mqttClient.publish('crypto/request/alert', JSON.stringify({ request_id: requestId, requester: 'dashboard' }), {
          qos: 1,
          properties: {
            responseTopic,
            correlationData: Buffer.from(requestId),
            messageExpiryInterval: 15,
          },
        });
        logFeature('Request-Response', `Dashboard mengirim request ke alertPublisher — menunggu response di "${responseTopic}"`);
      }
    } catch (e) { }
  });
});

// Serve static dashboard
app.use(express.static(path.join(__dirname, 'public')));

// Fitur 9: Shared Subscription — dashboard ikut bergabung ke group $share/loggers
// bersaing giliran dengan loggerSubscriber di terminal
const sharedSubClient = mqtt.connect(BROKER_URL, {
  clientId: 'dashboard-shared-monitor',
  protocolVersion: 5,
  clean: true,
  properties: { receiveMaximum: 20 },
});
sharedSubClient.on('connect', () => {
  console.log('✅ Shared Sub Monitor joined $share/loggers/crypto/alerts');
  sharedSubClient.subscribe('$share/loggers/crypto/alerts', { qos: 1 });
});
sharedSubClient.on('message', (topic, message) => {
  state.sharedSub.dashboardCount++;
  // loggerCount = totalAlerts yang masuk topic ini DIKURANGI yang diterima dashboard
  state.sharedSub.loggerCount = state.sharedSub.totalAlerts - state.sharedSub.dashboardCount;
  broadcast({ type: 'shared_sub_update', data: state.sharedSub });
  logFeature('Shared Subscription', `Dashboard giliran terima alert #${state.sharedSub.dashboardCount} dari total ${state.sharedSub.totalAlerts} di group "loggers"`);
});
sharedSubClient.on('error', (err) => console.error('Shared sub monitor error:', err.message));

// Fitur 10: Flow Control — hitung message rate setiap 5 detik lalu broadcast
setInterval(() => {
  state.flowControl.rate = (state.flowControl.msgCount / 5).toFixed(1);
  const count = state.flowControl.msgCount;
  state.flowControl.msgCount = 0;
  broadcast({ type: 'flow_control_update', data: { rate: state.flowControl.rate, receiveMaximum: state.flowControl.receiveMaximum } });
  if (count > 0) {
    logFeature('Flow Control', `receiveMaximum=50 aktif — ${count} msg/5s (${state.flowControl.rate} msg/s) diterima dashboard-server`);
  }
}, 5000);

server.listen(PORT, () => {
  console.log(`🌐 Dashboard running at http://localhost:${PORT}`);
});
