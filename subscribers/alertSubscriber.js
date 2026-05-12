// subscribers/alertSubscriber.js
// Subscriber 2: Alert Handler — khusus handle alert dan demonstrasikan Request-Response
// Mendemonstrasikan: QoS 2, Shared Subscription (Fitur 9), Request-Response (Fitur 8)

const mqtt = require('mqtt');

const BROKER_URL = 'mqtt://localhost:1883';
const RESPONSE_TOPIC = `crypto/response/alert-subscriber-${Date.now()}`;

const client = mqtt.connect(BROKER_URL, {
  clientId: 'subscriber-alert',
  protocolVersion: 5,
  clean: false,
  properties: { receiveMaximum: 10 },
});

client.on('connect', () => {
  console.log('✅ Alert Subscriber connected');

  // Subscribe ke alerts dengan QoS 2 (Exactly once — tidak boleh duplikat)
  client.subscribe('crypto/alerts', { qos: 2 }, () => {
    console.log('🔔 Subscribed to crypto/alerts (QoS 2)');
  });

  // Fitur 9: Shared Subscription — bergabung dengan loggers group untuk load balancing
  client.subscribe('$share/loggers/crypto/alerts', { qos: 1 }, () => {
    console.log('🔔 Joined shared subscription: $share/loggers/crypto/alerts');
  });

  // Subscribe ke response topic untuk Request-Response (Fitur 8)
  client.subscribe(RESPONSE_TOPIC, { qos: 1 }, () => {
    console.log(`🔔 Subscribed to response topic: ${RESPONSE_TOPIC}`);
  });

  // Subscribe ke status semua publisher (Fitur 2: single-level wildcard '+')
  // '+' hanya mengganti SATU level: crypto/status/price-publisher, crypto/status/alert-publisher, dll
  client.subscribe('crypto/status/+', { qos: 1 }, () => {
    console.log('🔔 Subscribed to crypto/status/+ (single-level wildcard)');
  });

  // Demo Request-Response: kirim request ke alert publisher setiap 30 detik (Fitur 8)
  setTimeout(sendAlertRequest, 5000);
  setInterval(sendAlertRequest, 30000);
});

function sendAlertRequest() {
  const requestId = `req-${Date.now()}`;
  const request = {
    request_id: requestId,
    requester: 'subscriber-alert',
    query: 'current_status',
    timestamp: new Date().toISOString(),
  };

  console.log(`\n📤 Sending Request-Response request (ID: ${requestId})`);

  // Fitur 8: Request-Response Pattern
  // Sertakan responseTopic dan correlationData di properties
  client.publish('crypto/request/alert', JSON.stringify(request), {
    qos: 1,
    properties: {
      responseTopic: RESPONSE_TOPIC,           // Fitur 8: kemana balasan dikirim
      correlationData: Buffer.from(requestId), // Fitur 8: untuk mencocokkan response
      messageExpiryInterval: 15,
    },
  });
}

client.on('message', (topic, message, packet) => {
  try {
    const data = JSON.parse(message.toString());
    const timestamp = new Date().toLocaleTimeString('id-ID');

    if (topic === 'crypto/alerts') {
      console.log(`\n[${timestamp}] 🚨 === ALERT RECEIVED (QoS ${packet.qos}) ===`);
      console.log(`  Type    : ${data.type}`);
      console.log(`  Coin    : ${data.symbol}`);
      console.log(`  Price   : $${data.price_usd}`);
      console.log(`  Change  : ${data.change_pct}%`);
      console.log(`  Level   : ${data.level}`);
      console.log(`  Message : ${data.message}`);
      console.log(`==========================================\n`);

    } else if (topic === RESPONSE_TOPIC) {
      // Fitur 8: Menerima response dari request
      const correlationId = packet.properties?.correlationData?.toString();
      console.log(`\n[${timestamp}] 📬 === REQUEST-RESPONSE RECEIVED ===`);
      console.log(`  Correlation ID : ${correlationId}`);
      console.log(`  Monitored      : ${data.monitored_coins?.join(', ')}`);
      console.log(`  Threshold      : ${data.threshold}%`);
      console.log(`  Request ID     : ${data.request_id}`);
      console.log(`=====================================\n`);

    } else if (topic.startsWith('crypto/status/')) {
      const publisher = topic.split('/')[2];
      const icon = data.status === 'online' ? '🟢' : '🔴';
      console.log(`[${timestamp}] ${icon} STATUS | ${publisher}: ${data.status} | Retain: ${packet.retain}`);
    }

  } catch (e) {
    console.log(`[RAW] ${topic}: ${message.toString()}`);
  }
});

client.on('error', (err) => console.error('Alert subscriber error:', err.message));
