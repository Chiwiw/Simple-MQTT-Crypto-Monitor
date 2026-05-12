// publishers/pricePublisher.js
// Publisher 1: Simulasi harga crypto real-time dan publish ke broker
// Mendemonstrasikan: QoS 1, Topic Alias (Fitur 3), User Properties (Fitur 4), Retain (Fitur 5)
// CATATAN: Menggunakan data simulasi karena CoinGecko API diblokir firewall lokal.

const mqtt = require('mqtt');

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

// Harga awal simulasi mendekati harga pasar nyata
const simulatedPrices = {
  bitcoin:  { price: 95000, market_cap: 1880000000000 },
  ethereum: { price: 1800,  market_cap: 216000000000  },
  solana:   { price: 148,   market_cap: 71000000000   },
};

// Fungsi random walk: gerakkan harga naik/turun kecil setiap tick
function nextPrice(coin) {
  const p = simulatedPrices[coin];
  const changePct = (Math.random() - 0.48) * 1.2; // sedikit bias naik
  p.price = parseFloat((p.price * (1 + changePct / 100)).toFixed(coin === 'bitcoin' ? 2 : coin === 'ethereum' ? 2 : 4));
  p.market_cap = parseFloat((p.market_cap * (1 + changePct / 100)).toFixed(0));
  return { price: p.price, market_cap: p.market_cap, change_24h: changePct.toFixed(2) };
}

let pollingInterval = null;

client.on('connect', () => {
  console.log('✅ Price Publisher connected to broker');
  console.log('📊 Mode: Simulated prices (CoinGecko diblokir firewall)');

  // Publish status online dengan retain
  client.publish(
    'crypto/status/price-publisher',
    JSON.stringify({ status: 'online', publisher: 'price-publisher', timestamp: new Date().toISOString() }),
    { qos: 1, retain: true } // Fitur 5: Retain
  );

  // Mulai simulasi harga setiap 10 detik (hanya set sekali)
  if (!pollingInterval) {
    publishPrices();
    pollingInterval = setInterval(publishPrices, 10000);
  }
});

function publishPrices() {
  for (const coin of COINS) {
    const { price, market_cap, change_24h } = nextPrice(coin);
    const topic = `crypto/price/${coin}`;
    const payload = {
      coin,
      symbol: SYMBOLS[coin],
      price_usd: price,
      change_24h,
      market_cap,
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
          source: 'simulated-data',
          unit: 'USD',
          publisher_version: '1.0.0',
        },
        // Fitur 6: Message Expiry — harga kadaluarsa setelah 30 detik
        messageExpiryInterval: 30,
      },
    });

    console.log(`📈 Published ${SYMBOLS[coin]}: $${price} (${change_24h}%)`);
  }
}

client.on('error', (err) => console.error('Publisher error:', err.message));
