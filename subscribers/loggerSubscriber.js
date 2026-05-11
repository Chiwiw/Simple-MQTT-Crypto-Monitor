// subscribers/loggerSubscriber.js
// Subscriber 1: Logger — mencatat semua data yang masuk ke console/file
// Mendemonstrasikan: Wildcard # (Fitur 2), Shared Subscription (Fitur 9)

const mqtt = require('mqtt');

const BROKER_URL = 'mqtt://localhost:1883';

const client = mqtt.connect(BROKER_URL, {
  clientId: 'subscriber-logger-1',
  clean: false,
  properties: {
    // Fitur 10: Flow Control — batasi berapa pesan yang diterima sebelum ACK
    receiveMaximum: 20,
  },
});

client.on('connect', () => {
  console.log('✅ Logger Subscriber connected');

  // Fitur 2: Wildcard '#' — subscribe ke SEMUA topic di bawah crypto/
  // Multi-level wildcard: mencakup crypto/price/bitcoin, crypto/alerts, crypto/market/global, dll
  client.subscribe('crypto/#', { qos: 1 }, () => {
    console.log('🔔 Subscribed to crypto/# (all topics)');
  });

  // Fitur 9: Shared Subscription — load balancing dengan subscriber-logger-2
  // Format: $share/<group_name>/<topic>
  // Hanya SALAH SATU dari logger-1 atau logger-2 yang akan menerima tiap pesan
  client.subscribe('$share/loggers/crypto/alerts', { qos: 1 }, () => {
    console.log('🔔 Joined shared subscription group: loggers (for alerts)');
  });
});

client.on('message', (topic, message, packet) => {
  try {
    const data = JSON.parse(message.toString());
    const timestamp = new Date().toLocaleTimeString('id-ID');

    // Log berdasarkan topic
    if (topic.startsWith('crypto/price/')) {
      const coin = topic.split('/')[2].toUpperCase();
      console.log(`[${timestamp}] 💰 PRICE  | ${coin}: $${data.price_usd} | 24h: ${data.change_24h}% | QoS: ${packet.qos}`);

    } else if (topic === 'crypto/alerts') {
      console.log(`[${timestamp}] 🚨 ALERT  | ${data.type} ${data.symbol} | ${data.change_pct}% | Level: ${data.level} | QoS: ${packet.qos}`);

    } else if (topic.startsWith('crypto/market/')) {
      const sub = topic.split('/')[2];
      console.log(`[${timestamp}] 🌍 MARKET | ${sub} | QoS: ${packet.qos}`);

    } else if (topic.startsWith('crypto/status/')) {
      const pub = topic.split('/')[2];
      console.log(`[${timestamp}] 📡 STATUS | ${pub}: ${data.status} (RETAINED: ${packet.retain})`);

    } else if (topic === 'crypto/errors') {
      console.log(`[${timestamp}] ❌ ERROR  | ${data.publisher}: ${data.error}`);
    }

    // Log User Properties jika ada (Fitur 4)
    if (packet.properties?.userProperties) {
      const props = packet.properties.userProperties;
      console.log(`         📎 Metadata: ${JSON.stringify(props)}`);
    }

    // Log Message Expiry jika ada (Fitur 6)
    if (packet.properties?.messageExpiryInterval) {
      console.log(`         ⏱️  Expires in: ${packet.properties.messageExpiryInterval}s`);
    }

  } catch (e) {
    // Non-JSON message
    console.log(`[RAW] ${topic}: ${message.toString()}`);
  }
});

client.on('error', (err) => console.error('Logger subscriber error:', err.message));
