# 🚀 MQTT Crypto Monitor
**Integrasi Sistem — Minggu 10**

Sistem monitoring harga crypto real-time menggunakan protokol MQTT. Data diambil dari **CoinGecko API** (gratis, tanpa API key) dan didistribusikan melalui MQTT broker ke semua subscriber.

---

## 📐 Arsitektur Sistem

```
CoinGecko API
     │
     ▼
┌─────────────────────────────────────────────┐
│              MQTT BROKER (Aedes)            │
│                 port: 1883                  │
└──────────┬──────────────────┬───────────────┘
           │                  │
    PUBLISH│                  │SUBSCRIBE
           │                  │
┌──────────▼──────┐   ┌───────▼──────────────┐
│   Publishers    │   │     Subscribers       │
│                 │   │                       │
│ 1. pricePublisher│   │ 1. loggerSubscriber   │
│    - BTC/ETH/SOL│   │    - Wildcard #       │
│    - QoS 1      │   │    - Shared Sub       │
│    - Retain     │   │                       │
│    - TopicAlias │   │ 2. alertSubscriber    │
│    - UserProps  │   │    - QoS 2            │
│    - MsgExpiry  │   │    - Request-Response │
│    - LWT        │   │    - Wildcard +       │
│                 │   └───────────────────────┘
│ 2. alertPublisher│          │
│    - QoS 2      │          │WEBSOCKET
│    - MsgExpiry  │          ▼
│    - LWT        │  ┌───────────────┐
│    - Req-Resp   │  │   Dashboard   │
│                 │  │  localhost:3000│
│ 3. marketPublish│  └───────────────┘
│    - QoS 0      │
│    - Retain     │
│    - LWT        │
└─────────────────┘
```

---

## ✅ 10 Fitur MQTT yang Diimplementasikan

| # | Fitur | Implementasi |
|---|-------|-------------|
| 1 | **Publish/Subscribe & QoS** | QoS 0 (market), QoS 1 (price), QoS 2 (alerts) |
| 2 | **Topic Wildcards** | `+` di alertPublisher, `#` di dashboard & loggerSubscriber |
| 3 | **Topic Alias** | pricePublisher memetakan nama topik panjang → integer ID |
| 4 | **User Properties** | Metadata `source`, `unit`, `publisher_version` di setiap publish |
| 5 | **Retain** | Harga terakhir & status publisher tersimpan di broker |
| 6 | **Message Expiry** | Alert expire 60s, harga expire 30s |
| 7 | **Last Will Testament** | Semua publisher punya LWT → broker publish "offline" jika mati |
| 8 | **Request-Response** | alertSubscriber & dashboard bisa query status ke alertPublisher |
| 9 | **Shared Subscription** | `$share/loggers/crypto/alerts` — load balancing antara 2 subscriber |
| 10 | **Flow Control** | `receiveMaximum` di semua client |

---

## 🛠️ Setup & Cara Menjalankan

### 1. Install dependencies
```bash
npm install
```

### 2. Jalankan semua komponen (buka terminal terpisah untuk tiap komponen)

**Terminal 1 — MQTT Broker:**
```bash
node broker/broker.js
```

**Terminal 2 — Dashboard Server:**
```bash
node dashboard/server.js
```

**Terminal 3 — Price Publisher:**
```bash
node publishers/pricePublisher.js
```

**Terminal 4 — Alert Publisher:**
```bash
node publishers/alertPublisher.js
```

**Terminal 5 — Market Publisher:**
```bash
node publishers/marketPublisher.js
```

**Terminal 6 — Logger Subscriber:**
```bash
node subscribers/loggerSubscriber.js
```

**Terminal 7 — Alert Subscriber:**
```bash
node subscribers/alertSubscriber.js
```

### 3. Buka Dashboard
```
http://localhost:3000
```

---

## 📁 Struktur Project

```
mqtt-crypto-monitor/
├── broker/
│   └── broker.js              # MQTT Broker (Aedes)
├── publishers/
│   ├── pricePublisher.js      # Publisher 1: Harga BTC/ETH/SOL
│   ├── alertPublisher.js      # Publisher 2: Alert pergerakan harga
│   └── marketPublisher.js     # Publisher 3: Global market stats
├── subscribers/
│   ├── loggerSubscriber.js    # Subscriber 1: Log semua data
│   └── alertSubscriber.js     # Subscriber 2: Handle alerts
├── dashboard/
│   ├── server.js              # HTTP + WebSocket bridge
│   └── public/
│       └── index.html         # Dashboard UI real-time
├── package.json
└── README.md
```

---

## 🔑 Topic Structure

```
crypto/
├── price/
│   ├── bitcoin        ← QoS 1, Retain, TopicAlias, UserProps, Expiry
│   ├── ethereum       ← QoS 1, Retain, TopicAlias, UserProps, Expiry
│   └── solana         ← QoS 1, Retain, TopicAlias, UserProps, Expiry
├── alerts             ← QoS 2, Expiry
├── market/
│   ├── global         ← QoS 0, Retain, Expiry
│   └── trending       ← QoS 0, Retain, Expiry
├── status/
│   ├── price-publisher    ← Retain, LWT
│   ├── alert-publisher    ← Retain, LWT
│   ├── market-publisher   ← Retain, LWT
│   └── dashboard          ← Retain, LWT
├── request/
│   └── alert          ← Request-Response (Fitur 8)
├── response/
│   └── <dynamic>      ← Response Topic (Fitur 8)
└── errors             ← QoS 1
```

---

## 💡 Tips Demo

- **LWT**: Matikan salah satu publisher dengan `Ctrl+C`, lihat status "offline" muncul di dashboard
- **Retain**: Start subscriber baru setelah publisher sudah jalan — data langsung muncul tanpa menunggu publish baru
- **QoS**: Perhatikan logger, QoS level berbeda untuk tiap tipe data
- **Shared Sub**: Logger-1 dan logger-2 berbagi beban alert (hanya salah satu yang terima)
- **Request-Response**: Klik tombol di dashboard untuk trigger real-time query
