# Data Reference — Binance API & Database Schema

Tài liệu tham khảo về cấu trúc dữ liệu: response từ Binance API, quá trình transform,
và schema cuối cùng trong ClickHouse.

---

## 1. Binance REST API

Base URL: `https://api.binance.com/api/v3`

### 1.1 Klines (Nến 1 phút)

**Endpoint:** `GET /klines`

**Tham số:**

| Tham số   | Giá trị                | Mô tả                        |
|-----------|------------------------|-------------------------------|
| symbol    | `BTCUSDT`, `ETHUSDT`…  | Cặp giao dịch                |
| interval  | `1m`                   | Khung thời gian (1 phút)      |
| limit     | `1–1000`               | Số nến trả về (mặc định 500)  |
| startTime | epoch ms               | Bắt đầu từ thời điểm         |
| endTime   | epoch ms               | Kết thúc tại thời điểm       |

**Response:** Mảng các mảng con (array of arrays), mỗi phần tử là 1 nến:

```json
[
  1779016320000,          // [0]  open_time      — epoch ms, thời điểm mở nến
  "78429.62000000",       // [1]  open           — giá mở
  "78429.64000000",       // [2]  high           — giá cao nhất
  "78429.62000000",       // [3]  low            — giá thấp nhất
  "78429.63000000",       // [4]  close          — giá đóng
  "1.38579000",           // [5]  volume         — khối lượng (base asset)
  1779016379999,          // [6]  close_time      — epoch ms, thời điểm đóng nến
  "108687.00334580",      // [7]  quote_volume   — khối lượng (quote asset, USDT)
  213,                    // [8]  trade_count    — số giao dịch trong nến
  "0.64271000",           // [9]  taker_buy_base — khối lượng mua (base)
  "50407.51389790",       // [10] taker_buy_quote— khối lượng mua (quote)
  "0"                     // [11] ignore         — không sử dụng
]
```

**Mapping sang cột CSV/DB:**

```
API index → CSV column       → ClickHouse column → Kiểu (DB)    → Ghi chú
──────────────────────────────────────────────────────────────────────────────
[0] open_time                → open_time          → DateTime      ← epoch ms → DateTime ở transform
[1] open                     → open               → Float64
[2] high                     → high               → Float64
[3] low                      → low                → Float64
[4] close                    → close              → Float64
[5] volume                   → volume             → Float64
[6] close_time               → close_time         → DateTime      ← epoch ms → DateTime ở transform
[7] quote_volume             → quote_volume       → Float64
[8] trade_count              → trade_count        → UInt32
[9] taker_buy_base           → taker_buy_base     → Float64
[10] taker_buy_quote         → taker_buy_quote    → Float64
[11] ignore                  → (bỏ)
```

**Lưu ý:**
- API trả positional array, không có key. Tên cột do pipeline tự đặt theo config
  (`_KLINES_RAW_COLUMNS` trong `config/config.py`).
- **Raw CSV giữ nguyên epoch ms** — không convert, không rename. Y nguyên như API trả về.
- **Transform** convert `open_time`, `close_time` từ epoch ms → DateTime, tính RSI/MACD,
  ghi Parquet vào `crypto-processed`.
- **Tất cả 11 cột** (trừ `ignore`) đều lưu vào ClickHouse.

---

### 1.2 Ticker 24hr

**Endpoint:** `GET /ticker/24hr`

**Tham số:**

| Tham số | Giá trị       | Mô tả           |
|---------|---------------|------------------|
| symbol  | `BTCUSDT`…    | Cặp giao dịch    |

**Response:** Object JSON (không phải array):

```json
{
  "symbol": "BTCUSDT",
  "priceChange": "273.56000000",       // Thay đổi giá 24h
  "priceChangePercent": "0.350",       // % thay đổi giá
  "weightedAvgPrice": "78123.326...",  // Giá trung bình (không dùng)
  "prevClosePrice": "78084.01000000",  // Giá đóng trước đó (không dùng)
  "lastPrice": "78357.57000000",       // Giá gần nhất (không dùng)
  "lastQty": "0.00227000",             // Khối lượng gần nhất (không dùng)
  "bidPrice": "78357.57000000",        // Giá mua tốt nhất
  "bidQty": "2.76427000",              // Khối lượng mua (không dùng)
  "askPrice": "78357.58000000",        // Giá bán tốt nhất
  "askQty": "1.59759000",              // Khối lượng bán (không dùng)
  "openPrice": "78084.01000000",       // Giá mở 24h (không dùng)
  "highPrice": "78599.99000000",       // Giá cao nhất 24h
  "lowPrice": "77721.19000000",        // Giá thấp nhất 24h
  "volume": "8172.45405000",           // Khối lượng 24h (base)
  "quoteVolume": "638459299.780...",   // Khối lượng 24h (USDT)
  "openTime": 1778930089001,           // Thời điểm bắt đầu (không dùng)
  "closeTime": 1779016489001,          // Thời điểm kết thúc (không dùng)
  "firstId": 6300550515,               // Trade ID đầu tiên (không dùng)
  "lastId": 6301744258,                // Trade ID cuối cùng (không dùng)
  "count": 1193744                     // Tổng số giao dịch 24h
}
```

**Mapping sang DB (sau transform):**

```
API field          → DB column          → Kiểu
──────────────────────────────────────────────────
symbol             → symbol             → String
(generated)        → snapshot_time      → DateTime (thời điểm pipeline chạy)
priceChange        → price_change       → Float64
priceChangePercent → price_change_pct   → Float64
highPrice          → high_24h           → Float64
lowPrice           → low_24h            → Float64
volume             → volume_24h         → Float64
quoteVolume        → quote_volume_24h   → Float64
count              → trade_count        → UInt32
bidPrice           → bid_price          → Float64
askPrice           → ask_price          → Float64
(computed)         → spread_pct         → Float64 = (ask - bid) / ask * 100
```

**Lưu ý:**
- API trả camelCase keys. Transform rename sang snake_case qua `BINANCE_COLUMN_MAP`.
- `snapshot_time` không có trong API — được gán = thời điểm pipeline chạy.
- `spread_pct` được tính: `(ask_price - bid_price) / ask_price * 100`.

---

### 1.3 Order Book (Depth)

**Endpoint:** `GET /depth`

**Tham số:**

| Tham số | Giá trị    | Mô tả                              |
|---------|------------|-------------------------------------|
| symbol  | `BTCUSDT`  | Cặp giao dịch                       |
| limit   | `5–100`    | Số mức giá mỗi bên (mặc định 100)   |

**Response:**

```json
{
  "lastUpdateId": 93824517016,
  "bids": [
    ["78357.57000000", "2.76447000"],   // [price, volume]
    ["78357.56000000", "0.00014000"],
    ...
  ],
  "asks": [
    ["78357.58000000", "1.59745000"],   // [price, volume]
    ["78357.59000000", "0.00071000"],
    ...
  ]
}
```

**Transform (tính toán từ bids/asks):**

```
Trường             → DB column          → Kiểu      → Cách tính
────────────────────────────────────────────────────────────────────
symbol             → symbol             → String
(generated)        → timestamp          → DateTime   = thời điểm pipeline chạy
(sum of bid vol)   → total_bid_volume   → Float64    = Σ bid[i][1]
(sum of ask vol)   → total_ask_volume   → Float64    = Σ ask[i][1]
(computed)         → imbalance          → Float64    = bid_vol / (bid_vol + ask_vol)
```

**Lưu ý:**
- `bids` và `asks` là mảng `[price, volume]`. Chỉ `volume` được dùng để tính.
- `imbalance` gần 1 = nhiều mua, gần 0 = nhiều bán, 0.5 = cân bằng.
- Raw bids/asks được lưu vào MinIO nhưng không lưu vào ClickHouse.

---

## 2. Data Vision (Historical Bootstrap)

**URL pattern:**
```
https://data.binance.vision/data/spot/monthly/klines/{SYMBOL}/1m/{SYMBOL}-1m-{YYYY}-{MM}.zip
```

**Ví dụ:**
```
https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/1m/BTCUSDT-1m-2024-01.zip
```

File ZIP chứa CSV với header giống positional array từ REST API.
Dùng cho bootstrap lần đầu (3 năm lịch sử). Sau đó pipeline dùng REST API cho incremental.

---

## 3. MinIO Object Storage

### 3.1 Buckets

| Bucket             | Mục đích                          |
|--------------------|-----------------------------------|
| `crypto-raw`       | Dữ liệu thô từ extract           |
| `crypto-processed` | Dữ liệu đã transform             |

### 3.2 Cấu trúc key

```
crypto-raw/
├── klines/{SYMBOL}/{YYYY-MM}.csv           ← nến 1 phút (CSV)
├── ticker_raw/{SYMBOL}/{YYYY-MM}.parquet   ← ticker 24h (Parquet)
└── order_book/{SYMBOL}/{YYYY-MM}.parquet   ← order book (Parquet)

crypto-processed/
├── klines/{SYMBOL}/{YYYY-MM}.parquet       ← nến + RSI/MACD (Parquet)
├── ticker_24h/{SYMBOL}/{YYYY-MM}.parquet   ← ticker đã transform (Parquet)
└── order_book_snapshot/{SYMBOL}/{YYYY-MM}.parquet ← order book đã transform (Parquet)
```

### 3.3 Cột dữ liệu trong MinIO

**Raw klines CSV** (`crypto-raw/klines/{S}/{M}.csv`):
```
open_time (epoch ms), open, high, low, close, volume,
close_time (epoch ms), quote_volume, trade_count,
taker_buy_base, taker_buy_quote
```

**Processed klines Parquet** (`crypto-processed/klines/{S}/{M}.parquet`):
```
symbol, open_time (DateTime), open, high, low, close, volume,
close_time (DateTime), quote_volume, trade_count,
taker_buy_base, taker_buy_quote, rsi_14, macd, macd_signal
```

**Raw ticker Parquet** (`crypto-raw/ticker_raw/{S}/{M}.parquet`):
```
symbol, priceChange, priceChangePercent, highPrice, lowPrice, volume,
quoteVolume, count, bidPrice, askPrice
```

**Processed ticker Parquet** (`crypto-processed/ticker_24h/{S}/{M}.parquet`):
```
symbol, snapshot_time, price_change, price_change_pct, high_24h, low_24h,
volume_24h, quote_volume_24h, trade_count, bid_price, ask_price, spread_pct
```

**Raw order book Parquet** (`crypto-raw/order_book/{S}/{M}.parquet`):
```
symbol, timestamp, bids, asks
```

**Processed order book Parquet** (`crypto-processed/order_book_snapshot/{S}/{M}.parquet`):
```
symbol, timestamp, total_bid_volume, total_ask_volume, imbalance
```

---

## 4. ClickHouse Database Schema

### 4.1 symbols (Dimension)

Bảng tĩnh, 50 coin theo dõi.

```sql
CREATE TABLE crypto_db.symbols (
    symbol       String,          -- "BTCUSDT"
    base_asset   String,          -- "BTC"
    quote_asset  String,          -- "USDT"
    status       String,          -- "TRADING" | "BREAK"
    created_at   DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree()
ORDER BY (symbol);
```

### 4.2 klines (Fact — nến 1 phút + chỉ báo)

```sql
CREATE TABLE crypto_db.klines (
    symbol          String,              -- "BTCUSDT"
    open_time       DateTime,            -- thời điểm mở nến (UTC)
    open            Float64,             -- giá mở
    high            Float64,             -- giá cao nhất
    low             Float64,             -- giá thấp nhất
    close           Float64,             -- giá đóng
    volume          Float64,             -- khối lượng (base asset)
    close_time      DateTime,            -- thời điểm đóng nến
    quote_volume    Float64,             -- khối lượng (USDT)
    trade_count     UInt32,              -- số giao dịch
    taker_buy_base  Float64,             -- khối lượng taker mua (base)
    taker_buy_quote Float64,             -- khối lượng taker mua (USDT)
    rsi_14          Nullable(Float64),   -- RSI(14)
    macd            Nullable(Float64),   -- MACD line
    macd_signal     Nullable(Float64)    -- MACD signal line
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(open_time)
ORDER BY (symbol, open_time);
```

**Chỉ báo kỹ thuật:**
- `rsi_14`: RSI(14) — SMA-based, EWM(span=14) trên gains/losses
- `macd`: MACD line = EMA(12) − EMA(26)
- `macd_signal`: Signal line = EMA(9) của MACD line

### 4.3 ticker_24h (Fact — snapshot ticker)

```sql
CREATE TABLE crypto_db.ticker_24h (
    symbol            String,     -- "BTCUSDT"
    snapshot_time     DateTime,   -- thời điểm pipeline chạy
    price_change      Float64,    -- thay đổi giá 24h
    price_change_pct  Float64,    -- % thay đổi giá
    high_24h          Float64,    -- giá cao nhất 24h
    low_24h           Float64,    -- giá thấp nhất 24h
    volume_24h        Float64,    -- khối lượng 24h (base)
    quote_volume_24h  Float64,    -- khối lượng 24h (USDT)
    trade_count       UInt32,     -- số giao dịch 24h
    bid_price         Float64,    -- giá mua tốt nhất
    ask_price         Float64,    -- giá bán tốt nhất
    spread_pct        Float64     -- spread % = (ask-bid)/ask * 100
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(snapshot_time)
ORDER BY (symbol, snapshot_time);
```

### 4.4 order_book_snapshot (Fact — snapshot sổ lệnh)

```sql
CREATE TABLE crypto_db.order_book_snapshot (
    symbol            String,     -- "BTCUSDT"
    timestamp         DateTime,   -- thời điểm pipeline chạy
    total_bid_volume  Float64,    -- tổng khối lượng mua (top N levels)
    total_ask_volume  Float64,    -- tổng khối lượng bán (top N levels)
    imbalance         Float64     -- bid_vol / (bid_vol + ask_vol)
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (symbol, timestamp);
```

### 4.5 chat_history (Chat AI)

```sql
CREATE TABLE crypto_db.chat_history (
    session_id       String,
    message_id       String,
    timestamp        DateTime DEFAULT now(),
    symbol           String,
    role             String,            -- "user" | "assistant"
    content          String,
    timeframe        Nullable(String),  -- "short" | "medium" | "long" | "very_long"
    context_summary  String DEFAULT ''
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (session_id, timestamp);
```

---

## 5. Pipeline Flow

```
┌─────────────────────────────────────────────────────────────────────┐
│                        EXTRACT (mỗi phút)                          │
│                                                                     │
│  Binance REST API ──▶ MinIO (crypto-raw)                            │
│  /klines        ──▶ klines/{SYMBOL}/{YYYY-MM}.csv                   │
│  /ticker/24hr   ──▶ ticker_raw/{SYMBOL}/{YYYY-MM}.parquet           │
│  /depth         ──▶ order_book/{SYMBOL}/{YYYY-MM}.parquet           │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                       TRANSFORM (mỗi phút)                         │
│                                                                     │
│  MinIO (crypto-raw) ──▶ Python compute ──▶ MinIO (crypto-processed) │
│                                                                     │
│  klines:      CSV + context → RSI(14) + MACD(12,26,9) → Parquet    │
│  ticker_raw:  Parquet       → rename + spread_pct      → Parquet   │
│  order_book:  Parquet       → bid/ask volumes          → Parquet   │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         LOAD (mỗi phút)                            │
│                                                                     │
│  MinIO (crypto-processed) ──▶ ClickHouse                            │
│                                                                     │
│  klines               ──▶ crypto_db.klines                          │
│  ticker_24h           ──▶ crypto_db.ticker_24h                      │
│  order_book_snapshot  ──▶ crypto_db.order_book_snapshot              │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 6. Cột không lưu vào DB

Chỉ một số cột từ ticker/24hr và depth API là không lưu:

| Cột                | API      | Lý do không lưu                           |
|--------------------|----------|-------------------------------------------|
| `weightedAvgPrice` | ticker   | Không dùng trong phân tích                |
| `prevClosePrice`   | ticker   | Không dùng trong phân tích                |
| `lastPrice`        | ticker   | Không dùng (dùng close từ klines)         |
| `openPrice`        | ticker   | Không dùng (dùng open từ klines)          |
| `lastQty`          | ticker   | Chi tiết quá                              |
| `bidQty`           | ticker   | Chi tiết quá                              |
| `askQty`           | ticker   | Chi tiết quá                              |
| `bids`, `asks`     | depth    | Chỉ dùng để tính volumes, không lưu raw   |

**Tất cả cột klines** (11/12, bỏ `ignore`) đều được lưu vào ClickHouse.
