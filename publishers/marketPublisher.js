// publishers/marketPublisher.js
// Publisher 3: Publish ringkasan market stats (fear & greed index, global market cap)
// Mendemonstrasikan: QoS 0, Retain, LWT, User Properties, Message Expiry

const mqtt = require('mqtt');
const axios = require('axios');

const BROKER_URL = 'mqtt://localhost:1883';

const client = mqtt.connect(BROKER_URL, {
  clientId: 'publisher-market',
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

client.on('connect', async () => {
  console.log('✅ Market Publisher connected');

  client.publish(
    'crypto/status/market-publisher',
    JSON.stringify({ status: 'online', publisher: 'market-publisher', timestamp: new Date().toISOString() }),
    { qos: 1, retain: true }
  );

  await fetchAndPublishMarket();
  setInterval(fetchAndPublishMarket, 30000); // Update tiap 30 detik
});

async function fetchAndPublishMarket() {
  try {
    // Fetch global market data
    const [globalRes, fearRes] = await Promise.all([
      axios.get('https://api.coingecko.com/api/v3/global', { timeout: 8000 }),
      axios.get('https://api.alternative.me/fng/', { timeout: 8000 }),
    ]);

    const global = globalRes.data.data;
    const fear = fearRes.data.data[0];

    const marketData = {
      total_market_cap_usd: global.total_market_cap?.usd,
      total_volume_usd: global.total_volume?.usd,
      btc_dominance: global.market_cap_percentage?.btc?.toFixed(2),
      eth_dominance: global.market_cap_percentage?.eth?.toFixed(2),
      active_cryptocurrencies: global.active_cryptocurrencies,
      market_cap_change_24h: global.market_cap_change_percentage_24h_usd?.toFixed(2),
      fear_greed_index: parseInt(fear.value),
      fear_greed_label: fear.value_classification,
      timestamp: new Date().toISOString(),
    };

    // Fitur 1: QoS 0 — market stats, tidak terlalu kritis jika hilang sesekali
    // Fitur 5: Retain — subscriber baru langsung dapat data terbaru
    // Fitur 6: Message Expiry — expired setelah 60 detik
    client.publish('crypto/market/global', JSON.stringify(marketData), {
      qos: 0,
      retain: true,
      properties: {
        messageExpiryInterval: 60,
        userProperties: {
          source: 'coingecko+alternative.me',
          update_interval: '30s',
          publisher_version: '1.0.0',
        },
      },
    });

    console.log(`🌍 Market published | BTC Dom: ${marketData.btc_dominance}% | Fear&Greed: ${marketData.fear_greed_index} (${marketData.fear_greed_label})`);

    // Publish trending coins menggunakan Wildcard topic hierarchy
    const trendingRes = await axios.get('https://api.coingecko.com/api/v3/search/trending', { timeout: 8000 });
    const trending = trendingRes.data.coins.slice(0, 5).map(c => ({
      name: c.item.name,
      symbol: c.item.symbol,
      market_cap_rank: c.item.market_cap_rank,
      score: c.item.score,
    }));

    client.publish('crypto/market/trending', JSON.stringify({ trending, timestamp: new Date().toISOString() }), {
      qos: 0,
      retain: true,
      properties: { messageExpiryInterval: 120 },
    });

    console.log(`🔥 Trending published: ${trending.map(c => c.symbol).join(', ')}`);

  } catch (err) {
    console.error('❌ Market fetch error:', err.message);
    client.publish(
      'crypto/errors',
      JSON.stringify({ publisher: 'market-publisher', error: err.message, timestamp: new Date().toISOString() }),
      { qos: 1 }
    );
  }
}

client.on('error', (err) => console.error('Market publisher error:', err.message));
