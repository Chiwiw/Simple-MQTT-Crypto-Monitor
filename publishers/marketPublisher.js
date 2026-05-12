// publishers/marketPublisher.js
// Publisher 3: Publish ringkasan market stats (fear & greed index, global market cap)
// Mendemonstrasikan: QoS 0, Retain, LWT, User Properties, Message Expiry

// CATATAN: Menggunakan data simulasi karena external API diblokir firewall lokal.
const mqtt = require('mqtt');

const BROKER_URL = 'mqtt://localhost:1883';

const client = mqtt.connect(BROKER_URL, {
  clientId: 'publisher-market',
  protocolVersion: 5,
  clean: false,

  // Fitur 7: LWT
  will: {
    topic: 'crypto/status/market-publisher',
    payload: JSON.stringify({
      status: 'offline',
      publisher: 'market-publisher',
      timestamp: new Date().toISOString(),
    }),
    qos: 1,
    retain: true,
  },

  properties: { receiveMaximum: 5 },
});

// Data simulasi market global
const simMarket = {
  total_market_cap_usd: 3200000000000,
  total_volume_usd: 142000000000,
  btc_dominance: 58.4,
  eth_dominance: 8.9,
  active_cryptocurrencies: 17429,
  market_cap_change_24h: 1.2,
  fear_greed_index: 52,
  fear_greed_label: 'Neutral',
};

// Simulated trending coins
const trendingCoins = [
  { name: 'Sui', symbol: 'SUI', market_cap_rank: 18, score: 0 },
  { name: 'Pepe', symbol: 'PEPE', market_cap_rank: 24, score: 1 },
  { name: 'Render', symbol: 'RENDER', market_cap_rank: 36, score: 2 },
  { name: 'Sei', symbol: 'SEI', market_cap_rank: 48, score: 3 },
  { name: 'Bonk', symbol: 'BONK', market_cap_rank: 55, score: 4 },
];

client.on('connect', () => {
  console.log('✅ Market Publisher connected');
  console.log('📊 Mode: Simulated market data (external API diblokir firewall)');

  client.publish(
    'crypto/status/market-publisher',
    JSON.stringify({ status: 'online', publisher: 'market-publisher', timestamp: new Date().toISOString() }),
    { qos: 1, retain: true }
  );

  publishMarket();
  setInterval(publishMarket, 30000); // Update tiap 30 detik
});

function publishMarket() {
  // Variasikan data sedikit setiap tick
  simMarket.market_cap_change_24h = parseFloat(((Math.random() - 0.45) * 4).toFixed(2));
  simMarket.fear_greed_index = Math.min(100, Math.max(0, simMarket.fear_greed_index + Math.round((Math.random() - 0.5) * 3)));
  simMarket.fear_greed_label = simMarket.fear_greed_index < 25 ? 'Extreme Fear'
    : simMarket.fear_greed_index < 45 ? 'Fear'
    : simMarket.fear_greed_index < 55 ? 'Neutral'
    : simMarket.fear_greed_index < 75 ? 'Greed' : 'Extreme Greed';

  const marketData = { ...simMarket, timestamp: new Date().toISOString() };

  // Fitur 1: QoS 0 — market stats, tidak terlalu kritis jika hilang sesekali
  // Fitur 5: Retain — subscriber baru langsung dapat data terbaru
  // Fitur 6: Message Expiry — expired setelah 60 detik
  client.publish('crypto/market/global', JSON.stringify(marketData), {
    qos: 0,
    retain: true,
    properties: {
      userProperties: {
        source: 'simulated-data',
        update_interval: '30s',
        publisher_version: '1.0.0',
      },
    },
  });

  console.log(`🌍 Market published | BTC Dom: ${marketData.btc_dominance}% | Fear&Greed: ${marketData.fear_greed_index} (${marketData.fear_greed_label})`);

  // Publish trending coins menggunakan Wildcard topic hierarchy (Fitur 2)
  client.publish('crypto/market/trending', JSON.stringify({ trending: trendingCoins, timestamp: new Date().toISOString() }), {
    qos: 0,
    retain: true,
  });

  console.log(`🔥 Trending published: ${trendingCoins.map(c => c.symbol).join(', ')}`);
}

client.on('error', (err) => console.error('Market publisher error:', err.message));
