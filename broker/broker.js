// broker/broker.js
// MQTT Broker lokal menggunakan Aedes (in-process broker)
//
// CATATAN: Aedes hanya mendukung MQTT v3.1 dan v3.1.1 (protocolVersion 3 & 4).
// Proyek ini menggunakan MQTT v5, sehingga semua client terhubung ke broker publik:
//   mqtt://broker.emqx.io:1883  (mendukung MQTT v5 penuh)
//
// File ini dijalankan sebagai referensi/arsitektur. Untuk demo penuh jalankan:
//   node dashboard/server.js  +  semua publisher & subscriber.

const { Aedes } = require('aedes');
const aedes = new Aedes();
const net = require('net');

const BROKER_PORT = 1883;

const server = net.createServer(aedes.handle);

server.listen(BROKER_PORT, () => {
  console.log(`✅ MQTT Broker running on port ${BROKER_PORT}`);
});

// Log setiap client yang connect
aedes.on('client', (client) => {
  console.log(`📡 Client connected: ${client.id}`);
});

// Log setiap client yang disconnect
aedes.on('clientDisconnect', (client) => {
  console.log(`🔌 Client disconnected: ${client.id}`);
});

// Log Last Will Testament saat client mati mendadak (LWT - Fitur 7)
aedes.on('clientError', (client, err) => {
  console.log(`💀 Client error (LWT will trigger): ${client?.id} - ${err.message}`);
});

// Log setiap publish
aedes.on('publish', (packet, client) => {
  if (client && packet.topic && !packet.topic.startsWith('$')) {
    console.log(`📨 [${client.id}] → topic: ${packet.topic} | QoS: ${packet.qos}`);
  }
});

// Log setiap subscribe
aedes.on('subscribe', (subscriptions, client) => {
  subscriptions.forEach(sub => {
    console.log(`🔔 [${client?.id}] subscribed to: ${sub.topic}`);
  });
});

module.exports = aedes;
