// publishers/alertPublisher.js
// Publisher 2: Monitor perubahan harga dan publish alert jika ada pergerakan signifikan
// Mendemonstrasikan: QoS 2, Message Expiry (Fitur 6), Request-Response (Fitur 8), LWT (Fitur 7)

const mqtt = require('mqtt');

const BROKER_URL = 'mqtt://localhost:1883';

// Threshold alert: jika perubahan harga > X%
const ALERT_THRESHOLD = 0.1; // 0.1% untuk demo — mudah ter-trigger dengan data simulasi

// Menyimpan harga sebelumnya untuk perbandingan
const previousPrices = {};

const client = mqtt.connect(BROKER_URL, {
  clientId: 'publisher-alert',
  protocolVersion: 5,
  clean: false,

  // Fitur 7: LWT — notif jika alert publisher mati
  will: {
    topic: 'crypto/status/alert-publisher',
    payload: JSON.stringify({
      status: 'offline',
      publisher: 'alert-publisher',
      message: '⚠️ Alert system is DOWN!',
      timestamp: new Date().toISOString(),
    }),
    qos: 2,
    retain: true,
  },

  properties: {
    receiveMaximum: 5,
  },
});

client.on('connect', () => {
  console.log('✅ Alert Publisher connected');

  // Status online
  client.publish(
    'crypto/status/alert-publisher',
    JSON.stringify({ status: 'online', publisher: 'alert-publisher', timestamp: new Date().toISOString() }),
    { qos: 1, retain: true }
  );

  // Subscribe ke semua harga crypto menggunakan Wildcard (Fitur 2)
  // '+' single-level wildcard: subscribe semua coin sekaligus
  client.subscribe('crypto/price/+', { qos: 1 }, () => {
    console.log('🔔 Subscribed to crypto/price/+ (wildcard)');
  });

  // Fitur 8: Request-Response — listen untuk request manual alert
  client.subscribe('crypto/request/alert', { qos: 1 }, () => {
    console.log('🔔 Subscribed to request/alert topic');
  });
});

client.on('message', (topic, message, packet) => {
  // Handle Request-Response (Fitur 8)
  if (topic === 'crypto/request/alert') {
    handleAlertRequest(message, packet);
    return;
  }

  // Handle price update
  if (topic.startsWith('crypto/price/')) {
    try {
      const data = JSON.parse(message.toString());
      checkAndPublishAlert(data);
    } catch (e) {
      console.error('Parse error:', e.message);
    }
  }
});

function checkAndPublishAlert(data) {
  const { coin, symbol, price_usd, change_24h } = data;

  if (!previousPrices[coin]) {
    previousPrices[coin] = price_usd;
    return;
  }

  const prev = previousPrices[coin];
  const change = ((price_usd - prev) / prev) * 100;
  previousPrices[coin] = price_usd;

  // Trigger alert jika perubahan melebihi threshold
  if (Math.abs(change) >= ALERT_THRESHOLD || Math.abs(parseFloat(change_24h)) >= 2) {
    const alertLevel = Math.abs(change) >= 2 ? 'HIGH' : 'MEDIUM';
    const direction = change > 0 ? '🚀 PUMP' : '📉 DUMP';

    const alert = {
      level: alertLevel,
      type: direction,
      coin,
      symbol,
      price_usd,
      change_pct: change.toFixed(3),
      change_24h,
      message: `${direction} detected on ${symbol}! Price: $${price_usd} (${change.toFixed(3)}%)`,
      timestamp: new Date().toISOString(),
    };

    // Fitur 1: QoS 2 — Exactly once (alert kritis, tidak boleh duplikat atau hilang)
    // Fitur 6: Message Expiry — alert kadaluarsa setelah 60 detik
    client.publish('crypto/alerts', JSON.stringify(alert), {
      qos: 2,
      properties: {
        messageExpiryInterval: 60, // Fitur 6: expired setelah 60 detik
        userProperties: {
          alert_level: alertLevel,
          publisher_version: '1.0.0',
        },
      },
    });

    console.log(`🚨 Alert published: ${alert.message}`);
  }
}

// Fitur 8: Request-Response Pattern
function handleAlertRequest(message, packet) {
  try {
    const request = JSON.parse(message.toString());
    const responseTopic = packet.properties?.responseTopic;
    const correlationData = packet.properties?.correlationData;

    console.log(`📬 Received alert request, responding to: ${responseTopic}`);

    const response = {
      status: 'ok',
      monitored_coins: Object.keys(previousPrices),
      current_prices: previousPrices,
      threshold: ALERT_THRESHOLD,
      timestamp: new Date().toISOString(),
      request_id: request.request_id,
    };

    if (responseTopic) {
      client.publish(responseTopic, JSON.stringify(response), {
        qos: 1,
        properties: {
          correlationData, // Fitur 8: Correlation Data untuk match request-response
          messageExpiryInterval: 30,
        },
      });
    }
  } catch (e) {
    console.error('Request handling error:', e.message);
  }
}

client.on('error', (err) => console.error('Alert publisher error:', err.message));
