// broker/broker.js
// MQTT Broker menggunakan Aedes (in-process broker)

const aedes = require('aedes')();
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
