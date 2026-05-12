// publishers/pricePublisher.js
// Publisher 1: Mengambil harga crypto dari CoinGecko API dan publish ke broker
// Mendemonstrasikan: QoS 1, Topic Alias (Fitur 3), User Properties (Fitur 4), Retain (Fitur 5)

const mqtt = require('mqtt');
const axios = require('axios');

const BROKER_URL = 'mqtt://localhost:1883';
const COINS = ['bitcoin', 'ethereum', 'solana'];
const SYMBOLS = { bitcoin: 'BTC', ethereum: 'ETH', solana: 'SOL' };

// Fitur 3: Topic Alias mapping (nama panjang → integer ID)
const TOPIC_ALIAS_MAP = {
  'crypto/price/bitcoin': 1,
  'crypto/price/ethereum': 2,
  'crypto/price/solana': 3,
};

const client = mqtt.connect(BROKER_URL, {
  clientId: 'publisher-price',
  protocolVersion: 5,
  clean: false, // Persistent session (Time Decoupling)

  // Fitur 7: Last Will Testament — broker akan publish "offline" jika publisher mati mendadak
  will: {
    topic: 'crypto/status/price-publisher',
    payload: JSON.stringify({
      status: 'offline',
      publisher: 'price-publisher',
      timestamp: new Date().toISOString(),
    }),
    qos: 1,
    retain: true, // Fitur 5: Retain — status terakhir tersimpan
  },

  properties: {
    // Fitur 10: Flow Control — batasi in-flight messages
    receiveMaximum: 10,
    // Fitur 3: Topic Alias Maximum
    topicAliasMaximum: 10,
  },
});

let pollingInterval = null;

client.on('connect', async () => {
  console.log('✅ Price Publisher connected to broker');

  // Publish status online dengan retain
  client.publish(
    'crypto/status/price-publisher',
    JSON.stringify({ status: 'online', publisher: 'price-publisher', timestamp: new Date().toISOString() }),
    { qos: 1, retain: true } // Fitur 5: Retain
  );

  // Mulai polling harga setiap 10 detik (hanya set sekali)
  if (!pollingInterval) {
    await fetchAndPublish();
    pollingInterval = setInterval(fetchAndPublish, 10000);
  }
});

async function fetchAndPublish() {
  try {
    let data;
    try {
      // Fetch harga real dari CoinGecko (gratis, no API key)
      const response = await axios.get(
        'https://api.coingecko.com/api/v3/simple/price',
        {
          params: {
            ids: COINS.join(','),
            vs_currencies: 'usd',
            include_24hr_change: true,
            include_market_cap: true,
          },
          timeout: 4000,
        }
      );
      data = response.data;
    } catch (apiErr) {
      console.log('⚠️ CoinGecko API error/rate-limit. Menggunakan data simulasi...');
      data = {
        bitcoin: { usd: 64000 + (Math.random() * 1000 - 500), usd_24h_change: (Math.random() * 10 - 5), usd_market_cap: 1200000000000 },
        ethereum: { usd: 3400 + (Math.random() * 100 - 50), usd_24h_change: (Math.random() * 10 - 5), usd_market_cap: 400000000000 },
        solana: { usd: 140 + (Math.random() * 10 - 5), usd_24h_change: (Math.random() * 10 - 5), usd_market_cap: 60000000000 }
      };
    }

    for (const coin of COINS) {
      if (!data[coin]) continue;

      const topic = `crypto/price/${coin}`;
      const payload = {
        coin,
        symbol: SYMBOLS[coin],
        price_usd: data[coin].usd,
        change_24h: data[coin].usd_24h_change?.toFixed(2),
        market_cap: data[coin].usd_market_cap,
        timestamp: new Date().toISOString(),
      };

      // Fitur 1: QoS 1 — At least once (harga penting, tidak boleh hilang)
      // Fitur 4: User Properties — metadata di luar payload
      // Fitur 5: Retain — subscriber baru langsung dapat harga terakhir
      client.publish(topic, JSON.stringify(payload), {
        qos: 1,
        retain: true, // Fitur 5
        properties: {
          // Fitur 3: Topic Alias
          topicAlias: TOPIC_ALIAS_MAP[topic],
          // Fitur 4: User Properties
          userProperties: {
            source: 'coingecko-api',
            unit: 'USD',
            publisher_version: '1.0.0',
          },
          // Fitur 6: Message Expiry — harga kadaluarsa setelah 30 detik
          messageExpiryInterval: 30,
        },
      });

      console.log(`📈 Published ${SYMBOLS[coin]}: $${data[coin].usd} (${data[coin].usd_24h_change?.toFixed(2)}%)`);
    }
  } catch (err) {
    console.error('❌ Failed to fetch prices:', err.message);

    // Publish error status
    client.publish(
      'crypto/errors',
      JSON.stringify({ publisher: 'price-publisher', error: err.message, timestamp: new Date().toISOString() }),
      { qos: 1 }
    );
  }
}

client.on('error', (err) => console.error('Publisher error:', err.message));
