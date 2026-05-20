# Hệ thống Data Pipeline Big Data & LLM Advisor - Tư vấn giao dịch Crypto

---

## 1. Tổng quan Dự án

### 1.1. Bối cảnh & Vấn đề

Thị trường cryptocurrency hoạt động 24/7 với khối lượng giao dịch khổng lồ. Việc phân tích và đưa ra quyết định giao dịch đòi hỏi:

- Xử lý dữ liệu lớn (hàng triệu records)
- Pipeline tự động, không can thiệp thủ công
- AI chatbot tương tác, phân tích đa chiều dựa trên dữ liệu thị trường thực

### 1.2. Mục tiêu Đồ án

Xây dựng hệ thống **End-to-End Data Pipeline** minh họa vai trò của Data Engineer + AI Engineer:

| Giai đoạn       | Công nghệ                 | Mục đích                                       |
| --------------- | ------------------------- | ---------------------------------------------- |
| **Extract**     | Python + Binance API      | Thu thập dữ liệu nến từ sàn giao dịch          |
| **Transform**   | ClickHouse SQL            | Tính toán chỉ số kỹ thuật (RSI, MACD) trong DB |
| **Load**        | ClickHouse                | Lưu trữ có cấu trúc, phục vụ analytics         |
| **Store**       | MinIO (S3-compatible)     | Object storage cho raw/processed data (Data Lake) |
| **Orchestrate** | Apache Airflow            | Tự động hóa và lập lịch các jobs               |
| **Chat**        | LLM (Gemini / OpenAI)    | Chatbot tư vấn thị trường tương tác      |
| **Visualize**   | Grafana                   | Dashboard theo dõi real-time                   |

### 1.3. Phạm vi & Giới hạn

| Thành phần        | Giá trị                   | Ghi chú                          |
| ----------------- | ------------------------- | -------------------------------- |
| Số lượng coin     | 50                        | Top vốn hóa, loại bỏ stablecoins |
| Khung thời gian   | Nến 1 phút                | Phù hợp phân tích ngắn hạn       |
| Dữ liệu lịch sử  | 3 năm (01/2023 - 01/2026) | Đủ để phân tích xu hướng          |
| AI Chat         | Chatbot tư vấn tương tác   | Dựa trên 30 nến daily + snapshot |

---

## 2. Danh sách 50 Coins

> **Tiêu chí chọn:** Top 50 theo vốn hóa thị trường, loại bỏ stablecoins (USDT, USDC, DAI) vì không có biến động giá.

| #   | Symbol    | #   | Symbol   | #   | Symbol     | #   | Symbol    | #   | Symbol   |
| --- | --------- | --- | -------- | --- | ---------- | --- | --------- | --- | -------- |
| 1   | BTCUSDT   | 11  | AVAXUSDT | 21  | NEARUSDT   | 31  | INJUSDT   | 41  | FTMUSDT  |
| 2   | ETHUSDT   | 12  | TONUSDT  | 22  | APTUSDT    | 32  | IMXUSDT   | 42  | ALGOUSDT |
| 3   | BNBUSDT   | 13  | SHIBUSDT | 23  | ICPUSDT    | 33  | OPUSDT    | 43  | FLOWUSDT |
| 4   | SOLUSDT   | 14  | XLMUSDT  | 24  | ETCUSDT    | 34  | GRTUSDT   | 44  | XTZUSDT  |
| 5   | XRPUSDT   | 15  | BCHUSDT  | 25  | STXUSDT    | 35  | THETAUSDT | 45  | AXSUSDT  |
| 6   | DOGEUSDT  | 16  | DOTUSDT  | 26  | RENDERUSDT | 36  | FILUSDT   | 46  | SANDUSDT |
| 7   | ADAUSDT   | 17  | UNIUSDT  | 27  | CROUSDT    | 37  | ARUSDT    | 47  | MANAUSDT |
| 8   | TRXUSDT   | 18  | LTCUSDT  | 28  | ATOMUSDT   | 38  | MKRUSDT   | 48  | NEOUSDT  |
| 9   | LINKUSDT  | 19  | HBARUSDT | 29  | VETUSDT    | 39  | WIFUSDT   | 49  | EOSUSDT  |
| 10  | MATICUSDT | 20  | PEPEUSDT | 30  | ARBUSDT    | 40  | RUNEUSDT  | 50  | AAVEUSDT |

---

## 3. Ước tính Khối lượng Dữ liệu

### 3.1. Bảng `klines` (Fact Table chính)

```
Số nến = 50 coins × 3 năm × 365 ngày × 24 giờ × 60 phút
       = 50 × 3 × 525,600
       = 78,840,000 records

Kích thước = 78.84 triệu × 100 bytes/record ≈ 7.5 GB
```

### 3.2. Bảng `ticker_24h` (Minutely snapshot)

```
Số records = 50 coins × 3 năm × 365 ngày × 24 giờ × 60 phút
           = 50 × 3 × 525,600
           = 78,840,000 records

Kích thước = 78.84 triệu × 150 bytes/record ≈ 11.2 GB
```

| Bảng                  | Số records     | Kích thước   | Tần suất ghi          |
| --------------------- | -------------- | ------------ | --------------------- |
| `symbols`             | 50             | < 1 KB       | 1 lần (setup)         |
| `klines`              | 78,840,000     | ~7.5 GB      | 50 recs/phút          |
| `ticker_24h`          | 78,840,000     | ~11.2 GB     | 50 recs/phút          |
| `order_book_snapshot`  | 78,840,000     | ~7.5 GB      | 50 recs/phút          |
| **Total**             | **~237 triệu** | **~26 GB**  | -                     |

### 3.5. Breakdown theo layer

| Layer                 | Format          | Kích thước | Ghi chú                               |
| --------------------- | --------------- | ---------- | ------------------------------------- |
| Raw (Data Lake)       | CSV → MinIO     | ~7.5 GB    | Dữ liệu gốc từ Binance               |
| Processed (Data Lake) | Parquet → MinIO | ~2 GB      | Nén tốt hơn CSV, có indicators        |
| Warehouse             | ClickHouse      | ~5 GB      | Bao gồm index + pre-aggregated data           |

---

## 4. Kiến trúc Hệ thống

### 4.1. Tổng quan Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                                 ORCHESTRATION                                    │
│                            ┌─────────────────────┐                              │
│                            │   Apache Airflow    │                              │
│                            │   (Scheduler)       │                              │
│                            └──────────┬──────────┘                              │
│                                       │ triggers                                │
├───────────────────────────────────────┼─────────────────────────────────────────┤
│                                       ▼                                         │
│  ┌─────────────┐    ┌─────────────────────────────────┐    ┌─────────────────┐ │
│  │   SOURCE    │    │           DATA LAKE             │    │ DATA WAREHOUSE  │ │
│  │             │    │  ┌───────────┐ ┌─────────────┐  │    │                 │ │
│  │  Binance    │───▶│  │  MinIO    │ │  MinIO      │  │───▶│   ClickHouse    │ │
│  │  API        │    │  │  Raw/CSV  │ │  Processed/ │  │    │                 │ │
│  │             │    │  │           │ │  Parquet    │  │    │                 │ │
│  └─────────────┘    │  └───────────┘ └─────────────┘  │    └────────┬────────┘ │
│                     │        │              ▲         │             │          │
│                     │        └─ ClickHouse ─┘         │             │          │
│                     └─────────────────────────────────┘             │          │
├─────────────────────────────────────────────────────────────────────┼──────────┤
│                                                                     ▼          │
│  ┌─────────────────────────────────┐    ┌─────────────────────────────────┐   │
│  │        LLM CHAT ASSISTANT       │    │         VISUALIZATION           │   │
│  │  ┌───────────┐ ┌─────────────┐  │    │                                 │   │
│  │  │  Gemini / │ │chat-api     │  │    │         Grafana                 │   │
│  │  │  OpenAI   │ │FastAPI (:8501)│  │    │         Dashboard (w/ iframe)   │   │
│  │  └───────────┘ └─────────────┘  │    │                                 │   │
│  └─────────────────────────────────┘    └─────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 4.2. Data Lake vs Data Warehouse

| Khía cạnh         | Data Lake                             | Data Warehouse              |
| ----------------- | ------------------------------------- | --------------------------- |
| **Vị trí**        | MinIO buckets (`crypto-raw`, `crypto-processed`) | ClickHouse database  |
| **Format**        | CSV (raw), Parquet (processed)        | Bảng SQL có schema          |
| **Schema**        | Schema-on-read (linh hoạt)            | Schema-on-write (cố định)  |
| **Mục đích**      | Lưu trữ, batch processing             | Query nhanh, serving        |
| **Công cụ xử lý** | ClickHouse SQL                       | SQL queries                 |
| **Ai dùng**       | Data Engineer, Data Scientist         | Analyst, Dashboard, LLM     |

### 4.3. Luồng dữ liệu chi tiết

```
[1] EXTRACT                    [2] TRANSFORM                 [3] LOAD
    Binance API                    ClickHouse SQL                ClickHouse
         │                              │                             │
         ▼                              ▼                             ▼
    ┌─────────┐                  ┌─────────────┐              ┌─────────────┐
    │  OHLCV  │   ClickHouse     │  + RSI      │   ClickHouse │   klines    │
    │  data   │  ──────────────▶ │  + MACD     │  ─────────▶  │   table     │
    │  (CSV)  │   SQL (s3)       │  (Parquet)  │   s3() read  │             │
    └─────────┘                  └─────────────┘              └─────────────┘

    MinIO: crypto-raw/           MinIO: crypto-processed/       crypto_db
    BTCUSDT.csv                  klines/{SYMBOL}/{YYYY-MM}.parquet
```

---

## 5. Database Schema

### 5.1. Tổng quan - Star Schema (4 bảng)

```
                         ┌─────────────────────┐
                         │      symbols        │
                         │  (Dimension Table)  │
                         │                     │
                         │  symbol (PK)        │
                         │  base_asset         │
                         │  quote_asset        │
                         │  status             │
                         └──────────┬──────────┘
                                    │
               ┌─────────────────────┼─────────────────────┐
               │                     │                     │
               ▼                     ▼                     ▼
 ┌──────────────────────┐ ┌──────────────────────┐ ┌──────────────────────────┐
 │       klines         │ │     ticker_24h       │ │   order_book_snapshot    │
 │    (Fact Table)      │ │    (Fact Table)      │ │       (Fact Table)       │
 │                      │ │                      │ │                          │
 │  symbol (FK) ────────│─│── symbol (FK) ───────│─│── symbol (FK)            │
 │  timestamp (PK)      │ │  snapshot_time (PK)  │ │  timestamp (PK)          │
 │  open, high, low...  │ │  price_change_24h    │ │  total_bid_volume        │
 │  rsi, macd...        │ │  volume_24h...       │ │  total_ask_volume        │
 └──────────────────────┘ └──────────────────────┘ │  imbalance               │
                                                   └──────────────────────────┘

> **Thiết kế Star Schema:** 1 Dimension table (`symbols`) + 3 Fact tables (`klines`, `ticker_24h`, `order_book_snapshot`)

### 5.2. Nguồn dữ liệu

| Bảng                  | Nguồn dữ liệu                                             | Mô tả                                    | Tần suất thu thập |
| --------------------- | ---------------------------------------------------------- | ---------------------------------------- | ----------------- |
| `symbols`             | Binance `/api/v3/exchangeInfo`                             | Thông tin trading pair                   | 1 lần (setup)     |
| `klines`              | Binance `/api/v3/klines`                                   | Dữ liệu nến (OHLCV) + indicators        | Mỗi phút          |
| `ticker_24h`          | Binance `/api/v3/ticker/24hr` + `/api/v3/ticker/bookTicker`| Thống kê 24h + best bid/ask + spread     | Mỗi phút          |
| `order_book_snapshot`  | Binance `/api/v3/depth`                                    | Snapshot order book để đo áp lực mua/bán | Mỗi phút          |

### 5.3. Bảng `symbols` (Dimension Table)

> **Mục đích:** Lưu thông tin tĩnh về 50 coins, tránh lặp lại trong các Fact tables.

| Column        | Type     | Description                 | Nguồn                |
| ------------- | -------- | --------------------------- | -------------------- |
| `symbol`      | String   | Cặp giao dịch (PK)          | Binance exchangeInfo |
| `base_asset`  | String   | Coin gốc (BTC, ETH...)      | Binance exchangeInfo |
| `quote_asset` | String   | Coin định giá (USDT)        | Binance exchangeInfo |
| `status`      | String   | Trạng thái (TRADING, BREAK) | Binance exchangeInfo |
| `created_at`  | DateTime | Thời gian thêm vào hệ thống | System               |

**Primary Key (ORDER BY):** `symbol`

**Engine:** `ReplacingMergeTree()`

**Sample Data:**

```
| symbol   | base_asset | quote_asset | status  | created_at          |
|----------|------------|-------------|---------|---------------------|
| BTCUSDT  | BTC        | USDT        | TRADING | 2023-01-01 00:00:00 |
| ETHUSDT  | ETH        | USDT        | TRADING | 2023-01-01 00:00:00 |
```

### 5.4. Bảng `klines` (Fact Table)

> **Mục đích:** Lưu dữ liệu nến 1 phút và các chỉ số kỹ thuật đã tính toán.

| Column         | Type             | Description          | Nguồn           |
| -------------- | ---------------- | -------------------- | --------------- |
| `symbol`       | String           | → symbols.symbol     | Binance         |
| `timestamp`    | DateTime         | Thời gian mở nến     | Binance         |
| `open`         | Float64          | Giá mở cửa           | Binance         |
| `high`         | Float64          | Giá cao nhất         | Binance         |
| `low`          | Float64          | Giá thấp nhất        | Binance         |
| `close`        | Float64          | Giá đóng cửa         | Binance         |
| `volume`       | Float64          | Khối lượng giao dịch | Binance         |
| `quote_volume` | Float64          | Volume (USDT)        | Binance         |
| `trades`       | UInt32           | Số lượng trades      | Binance         |
| `rsi_14`       | Nullable(Float64)| RSI 14 periods       | ClickHouse SQL Transform |
| `macd`         | Nullable(Float64)| MACD line            | ClickHouse SQL Transform |
| `macd_signal`  | Nullable(Float64)| MACD signal line     | ClickHouse SQL Transform |

**ORDER BY:** `(symbol, timestamp)`

**PARTITION BY:** `toYYYYMM(timestamp)`

**Engine:** `ReplacingMergeTree()`

### 5.5. Bảng `ticker_24h` (Fact Table)

> **Mục đích:** Lưu snapshot thống kê 24h mỗi ngày, dùng để phân tích volume trend và market sentiment.

| Column             | Type    | Description                       | Nguồn               |
| ------------------ | ------- | --------------------------------- | ------------------- |
| `symbol`           | String  | → symbols.symbol                  | Binance ticker/24hr |
| `snapshot_time`    | DateTime| Thời điểm lấy snapshot            | System              |
| `price_change`     | Float64 | Chênh lệch giá 24h (absolute)     | Binance ticker/24hr |
| `price_change_pct` | Float64 | Chênh lệch giá 24h (%)            | Binance ticker/24hr |
| `high_24h`         | Float64 | Giá cao nhất trong 24h            | Binance ticker/24hr |
| `low_24h`          | Float64 | Giá thấp nhất trong 24h           | Binance ticker/24hr |
| `volume_24h`       | Float64 | Volume giao dịch 24h (base asset) | Binance ticker/24hr |
| `quote_volume_24h` | Float64 | Volume 24h (quote asset - USDT)   | Binance ticker/24hr |
| `trade_count`      | UInt32  | Số lượng trades trong 24h         | Binance ticker/24hr |
| `bid_price`        | Float64 | Best bid                          | Binance bookTicker  |
| `ask_price`        | Float64 | Best ask                          | Binance bookTicker  |
| `spread_pct`       | Float64 | (ask - bid) / ask \* 100          | Calculated          |

**ORDER BY:** `(symbol, snapshot_time)`

**Engine:** `ReplacingMergeTree()`

**Sample Data:**

```
| symbol  | snapshot_time       | price_change_pct | volume_24h    | trade_count |
|---------|---------------------|------------------|---------------|-------------|
| BTCUSDT | 2026-01-26 00:00:00 | 2.35             | 45,230.5 BTC  | 1,245,678   |
| ETHUSDT | 2026-01-26 00:00:00 | -1.20            | 312,450.2 ETH | 892,345     |
```

> **Ý nghĩa:** Dữ liệu này giúp phân tích market sentiment, detect unusual volume spikes, và là context đầu vào cho LLM advisory.

### 5.6. Bảng `order_book_snapshot` (Fact Table)

> **Mục đích:** Theo dõi áp lực thanh khoản ngắn hạn (Live Liquidity Pressure) từ order book snapshot.

| Column                     | Type               | Description                                     | Nguồn         |
| -------------------------- | ------------------ | ----------------------------------------------- | ------------- |
| `symbol`                   | String             | → symbols.symbol                                | Binance depth |
| `timestamp`                | DateTime           | Thời điểm snapshot                              | System        |
| `best_bid`                 | Float64            | Giá bid cao nhất                                | Binance depth |
| `best_ask`                 | Float64            | Giá ask thấp nhất                               | Binance depth |
| `mid_price`                | Float64            | (best_bid + best_ask) / 2                       | Calculated    |
| `spread_pct`               | Float64            | (best_ask - best_bid) / mid_price * 100         | Calculated    |
| `depth_bid_volume`         | Float64            | Tổng volume bid trong ±0.5% mid price           | Calculated    |
| `depth_ask_volume`         | Float64            | Tổng volume ask trong ±0.5% mid price           | Calculated    |
| `obi`                      | Float64            | (bid - ask) / (bid + ask), range -1 → +1        | Calculated    |
| `bid_ask_ratio`            | Float64            | depth_bid_volume / depth_ask_volume             | Calculated    |
| `nearest_bid_wall_price`   | Nullable(Float64)  | Giá level có volume bid lớn nhất trong depth    | Calculated    |
| `nearest_bid_wall_volume`  | Nullable(Float64)  | Volume của bid wall đó                          | Calculated    |
| `nearest_ask_wall_price`   | Nullable(Float64)  | Giá level có volume ask lớn nhất trong depth    | Calculated    |
| `nearest_ask_wall_volume`  | Nullable(Float64)  | Volume của ask wall đó                          | Calculated    |

**ORDER BY:** `(symbol, timestamp)`

**Engine:** `ReplacingMergeTree()`

> **Ý nghĩa:** OBI > 0 → bid-side liquidity mạnh hơn (áp lực mua), OBI < 0 → ask-side liquidity mạnh hơn (áp lực bán). Volume được tính trong phạm vi ±0.5% quanh mid price để phản ánh áp lực thanh khoản tức thời, không bao gồm các level quá xa giá. Bid/Ask wall chỉ được ghi nhận khi volume level đó ≥ 3× trung bình các level trong depth range. Dữ liệu này là một trong các input cho LLM advisory.

### 5.7. Ví dụ SQL Queries (ClickHouse)

**Query 1:** Snapshot mới nhất theo symbol: close + ticker

```sql
SELECT
    s.symbol,
    s.base_asset,
    k.latest_close,
    t.price_change_pct AS change_24h_pct
FROM symbols s
LEFT JOIN (
    SELECT symbol, argMax(close, timestamp) AS latest_close
    FROM klines GROUP BY symbol
) k ON s.symbol = k.symbol
LEFT JOIN (
    SELECT symbol, argMax(price_change_pct, snapshot_time) AS price_change_pct
    FROM ticker_24h GROUP BY symbol
) t ON s.symbol = t.symbol
ORDER BY s.symbol;
```

### 5.9. Giải thích các chỉ số kỹ thuật

| Chỉ số          | Công thức          | Ý nghĩa                                         |
| --------------- | ------------------ | ----------------------------------------------- |
| **RSI (14)**    | 100 - 100/(1 + RS) | Đo momentum, > 70 = overbought, < 30 = oversold |
| **MACD**        | EMA(12) - EMA(26)  | Đo xu hướng và momentum                         |
| **MACD Signal** | EMA(9) của MACD    | Tín hiệu mua/bán khi MACD cắt Signal            |

---

## 6. ELT Pipeline

### 6.1. Tổng quan luồng dữ liệu (4 bảng)

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                                    EXTRACT                                               │
│                                                                                          │
│   ┌─────────────────┐   ┌─────────────────────┐   ┌─────────────────────────┐   ┌───────────────────┐ │
│   │ /exchangeInfo   │   │    /klines          │   │    /ticker/24hr         │   │    /depth         │ │
│   │ (1 lần setup)   │   │ (Minutely: 50 coins)│   │ (Minutely: 50 coins)    │   │ (Minutely)         │ │
│   └────────┬────────┘   └──────────┬──────────┘   └────────────┬────────────┘   └─────────┬─────────┘ │
│            │                       │                           │                           │           │
│            ▼                       ▼                           ▼                           ▼           │
│    config/symbols.py       MinIO: crypto-raw/           ticker_24h.csv           order_book_snapshot.csv │
└────────────┬───────────────────────┬───────────────────────────┬───────────────────────────┬───────────┘
             │                       │                           │
             │              ┌────────┴────────┐                  │
             │              │    TRANSFORM    │                  │
             │              │   (ClickHouse SQL)       │                  │
             │              │  + RSI, MACD    │                  │
             │              └────────┬────────┘                  │
             │                       │                           │
             │                       ▼                           │
             │           MinIO: crypto-processed/                │
             │              klines/{SYMBOL}/{YYYY-MM}.parquet          │
             │                       │                           │
┌────────────┴───────────────────────┴───────────────────────────┴────────────────────────┐
│                                      LOAD                                                │
│                                                                                          │
│    ┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐   ┌──────────────────────┐ │
│    │    symbols      │   │     klines      │   │   ticker_24h    │   │ order_book_snapshot │ │
│    │  (dim table)    │   │  (fact table)   │   │  (fact table)   │   │   (fact table)      │ │
│    └─────────────────┘   └─────────────────┘   └─────────────────┘   └──────────────────────┘ │
│                                                                                          │
│                              ClickHouse Database                                         │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

### 6.2. Extract - Thu thập dữ liệu

#### 6.2.1. Symbols Registry (1 lần khi setup)

| Thuộc tính | Giá trị                 |
| ---------- | ----------------------- |
| API        | Không gọi API; đọc `SYMBOL_REGISTRY` |
| Tần suất   | 1 lần (initial setup)   |
| Output     | Bảng `symbols`          |
| Ghi vào    | Bảng `symbols`          |

#### 6.2.2. Extract Klines (Minutely)

| Thuộc tính | Giá trị                                                                |
| ---------- | ---------------------------------------------------------------------- |
| API        | `/api/v3/klines`                                                       |
| Tần suất   | Mỗi phút (* * * * *)                                                   |
| Chiến lược | Bulk download từ Binance Data Vision (lịch sử), REST API (dữ liệu mới) |
| Output     | MinIO bucket `crypto-raw/klines/{SYMBOL}/{YYYY-MM}.csv`               |
| Ghi vào    | Bảng `klines` (sau Transform)                                          |

> **Lưu ý:** Extract initial nặng (bulk 3 năm × 50 coins = 78M rows). Sau khi bulk xong, mỗi phút chỉ lấy 1 nến mới/coin (~50 rows) từ REST API → rất nhẹ.

#### 6.2.3. Extract Ticker 24h + Best Bid/Ask (Minutely)

| Thuộc tính | Giá trị                                            |
| ---------- | -------------------------------------------------- |
| API        | `/api/v3/ticker/24hr`, `/api/v3/ticker/bookTicker` |
| Tần suất   | Mỗi phút (cùng minutely_etl DAG)                   |
| Đặc điểm   | 1 request lấy được tất cả 50 coins                 |
| Output     | `data/raw/ticker_24h.csv`                          |
| Ghi vào    | Bảng `ticker_24h`                                  |

> **Lưu ý:** API ticker/24hr và bookTicker trả về rolling 24h snapshot tại thời điểm gọi. Mỗi phút lưu 1 record/coin, giúp theo dõi biến động gần real-time.

#### 6.2.4. Extract Order Book Snapshot (Minutely)

| Thuộc tính | Giá trị                            |
| ---------- | ---------------------------------- |
| API        | `/api/v3/depth`                    |
| Tần suất   | Mỗi phút, cùng minutely_etl DAG   |
| Đặc điểm   | Snapshot top N levels              |
| Output     | `data/raw/order_book_snapshot.csv` |
| Ghi vào    | Bảng `order_book_snapshot`         |

> **Lưu ý:** Lấy snapshot mỗi phút để theo dõi áp lực mua/bán gần real-time. Mỗi lần chỉ cần 1–2 API requests (trả về tất cả 50 coins) → overhead rất thấp.

### 6.3. Transform - Xử lý với ClickHouse SQL

**Input:** Raw CSV files từ Data Lake (MinIO `crypto-raw/klines/{SYMBOL}/{YYYY-MM}.csv`)

**Xử lý:**

1. ClickHouse SQL đọc raw CSV qua table function `s3()`
2. Fetch context rows (120 nến gần nhất) từ bảng `klines` để warm-up indicators
3. Tính RSI(14) + MACD(12,26,9) bằng SQL window functions:
   - RSI: `avg(if(delta>0, delta, 0)) OVER (ROWS 13 PRECEDING)`
   - EMA: `exponentialMovingAverage()` với halflife tính từ span
   - MACD: EMA(12) - EMA(26), Signal: EMA(9) của MACD

**Output:** MinIO `crypto-processed/klines/{SYMBOL}/{YYYY-MM}.parquet` (ghi qua `INSERT INTO FUNCTION s3()`)

> **Tại sao dùng ClickHouse SQL?** Transform được đẩy xuống database (ELT pattern), tận dụng ClickHouse columnar engine + vectorized computation. Không cần Spark cluster, giảm infrastructure complexity. Dữ liệu 78 triệu records được xử lý trực tiếp trong ClickHouse mà không cần di chuyển qua Python.

### 6.4. Load - Ghi vào ClickHouse

| Bảng                  | Input                   | Mode   | Ghi chú                              |
| --------------------- | ----------------------- | ------ | ------------------------------------- |
| `symbols`             | `SYMBOL_REGISTRY`       | Upsert | 1 lần setup, update nếu cần           |
| `klines`              | klines/{SYMBOL}/{YYYY-MM}.parquet | Append | Đọc processed Parquet qua s3(), INSERT  |
| `ticker_24h`          | ticker_24h.csv          | Append | 50 records/phút (minutely_etl)        |
| `order_book_snapshot`  | order_book_snapshot.csv | Append | 50 records/phút (minutely_etl)        |

---

## 7. Job Scheduling với Apache Airflow

### 7.1. Tại sao cần Airflow?

| Vấn đề khi chạy thủ công  | Giải pháp Airflow    |
| ------------------------- | -------------------- |
| Quên chạy ETL             | Tự động theo lịch    |
| Job A phải chờ job B xong | Dependencies rõ ràng |
| Không biết lỗi ở đâu      | Logs, alerts, retry  |
| Khó theo dõi tiến độ      | Web UI visualization |

### 7.2. Danh sách DAGs

| DAG                  | Schedule        | Mô tả                                                  |
| -------------------- | --------------- | ------------------------------------------------------- |
| `minutely_etl`       | * * * * *       | Extract klines/ticker/order book → Transform → Load     |

### 7.3. DAG: minutely_etl

```
Trigger: Mỗi phút (* * * * *)
Timeout: 3 phút
max_active_runs: 1 (tránh overlap)

Note: ETL nặng chỉ lần đầu (bulk 3 năm × 50 coins = 78M rows, dùng Data Vision).
      Incremental mỗi phút chỉ xử lý ~50 rows (1 nến/coin) → rất nhẹ.

┌──────────────────┐     ┌──────────────────┐     ┌────────────────────┐
│ extract_klines   │     │ extract_ticker   │     │ extract_order_book │
│                  │     │                  │     │                    │
│ - GET /klines    │     │ - GET /ticker    │     │ - GET /depth       │
│ - 50 coins       │     │ - GET /bookTicker│     │ - 50 coins         │
└────────┬─────────┘     └────────┬─────────┘     └─────────┬──────────┘
         │                        │                          │
         ▼                        ▼                          ▼
┌──────────────────┐     ┌──────────────────┐     ┌────────────────────┐
│ transform_klines │     │   load_ticker    │     │  load_order_book   │
│                  │     │                  │     │                    │
│ - ClickHouse SQL│     │ - ClickHouse     │     │ - ClickHouse       │
│ - RSI, MACD → MinIO│     │ - ticker_24h     │     │ - order_book table │
└────────┬─────────┘     └──────────────────┘     └────────────────────┘
         │
         ▼
┌──────────────────┐
│  load_klines     │
│                  │
│ - ClickHouse s3()│
│ - klines table   │
└──────────────────┘

(3 nhánh extract chạy song song)
```

> **Dependencies:** `extract_klines >> transform_klines >> load_klines`, `extract_ticker >> load_ticker`, `extract_order_book >> load_order_book`

---

## 8. LLM Chat Assistant — AI Chat Tương Tác

### 8.1. Thiết kế tổng quan

**Bài toán:** Hệ thống cung cấp một AI chatbot linh hoạt, cho phép người dùng đặt câu hỏi tự do về thị trường và nhận được phân tích chuyên sâu dựa trên dữ liệu thật.

**Kiến trúc:** Iframe trên Grafana → truy cập FastAPI Backend (`chat-api`) → tự động query ClickHouse (30 market candles gần nhất + snapshot thị trường) → Gửi prompt tới LLM (Gemini/OpenAI) → Trả về Markdown response cho UI.

### 8.2. Cấu hình LLM cho Chat

| Parameter            | Value               | Giải thích                                       |
| -------------------- | ------------------- | ------------------------------------------------ |
| Daily candles        | 30                  | 30 nến daily (~1 tháng) tự động làm context      |
| Providers            | Gemini / OpenAI     | Có thể switch qua env variable                   |
| Gemini model         | gemini-2.5-flash-lite| Nhanh, rẻ, phù hợp cho text chat                |
| OpenAI model         | gpt-5.4-nano        | Alternative provider                             |
| Temperature          | 0.3                 | Tăng một chút so với 0.1 để câu trả lời tự nhiên hơn|
| Max tokens           | 512                 | Đủ cho một response phân tích chi tiết           |
| Timeout              | 30s / request       | Tăng timeout cho chat response dài               |
| History              | 10 turn             | Giữ ngữ cảnh chat (context memory) cho session   |

### 8.3. Context Đầu Vào Tự Động (System Prompt)

Mỗi lần người dùng gửi tin nhắn, backend tự động fetch dữ liệu mới nhất từ ClickHouse để chèn vào `system_prompt`:

| Dữ liệu             | Nguồn                | Chi tiết                                    |
| -------------------- | -------------------- | ------------------------------------------- |
| Daily candles        | Bảng `klines`        | OHLCV, RSI, MACD của 30 ngày gần nhất       |
| MACD crossover       | Bảng `klines`        | bullish / bearish / neutral                 |
| Order book liquidity  | Bảng `order_book_snapshot` | OBI ±1, spread, bid/ask walls, ratio |
| Price change 24h     | Bảng `ticker_24h`    | % thay đổi giá                              |
| Volume change 24h    | Bảng `ticker_24h`    | % thay đổi volume so với ngày trước         |

### 8.4. Giao diện & Fallback

| Tính năng | Mô tả |
|---|---|
| **Dark Theme** | Giao diện chat (HTML/CSS) được build dark-theme để nhúng liền mạch vào Grafana. |
| **Typing Indicator** | Hiệu ứng UI "đang gõ..." trong lúc chờ LLM xử lý. |
| **Markdown Parsing** | Frontend (marked.js) render bôi đậm, danh sách và bảng biểu. |
| **Quota Fallback** | Báo lỗi thân thiện nếu API Key hết quota thay vì crash. |

---

## 9. Infrastructure

### 9.1. Docker Services

| Service              | Image                            | Port       | Mục đích                    |
| -------------------- | -------------------------------- | ---------- | --------------------------- |
| ClickHouse           | clickhouse/clickhouse-server:24.3| 8123, 9100 | Data Warehouse              |
| MinIO                | minio/minio:latest               | 9000, 9001 | Object Storage (Data Lake)  |
| MinIO Init           | minio/mc:latest                  | -          | Tạo buckets lần đầu         |
| Airflow PostgreSQL   | postgres:15                      | -          | Airflow metadata DB         |
| Airflow Init         | apache/airflow:2.8.0             | -          | DB migrate + admin user     |
| Airflow Webserver    | apache/airflow:2.8.0             | 8080       | DAG UI                      |
| Airflow Scheduler    | apache/airflow:2.8.0             | -          | Chạy DAGs                   |
| Grafana              | grafana/grafana:11.4.0           | 3000       | Dashboard                   |

### 9.2. Native Services (chạy trên host)

| Component | Lý do không Docker                      |
| --------- | --------------------------------------- |
| LLM API   | Chỉ cần HTTP call, không cần container   |

### 9.3. Cấu trúc thư mục

```
crypto-pipeline/
├── config/
│   ├── config.py               # Centralized config (paths, DB, API, MinIO, ClickHouse)
│   ├── llm_config.py           # LLM config (provider, model, chat params)
│   └── symbols.py              # SYMBOL_REGISTRY (50 coins, single source of truth)
├── airflow/
│   └── dags/
│       └── minutely_etl.py     # Mini-batch ETL mỗi phút (* * * * *)
├── services/
│   └── chat_api/               # LLM Chat Assistant backend
│       ├── Dockerfile
│       ├── requirements.txt
│       ├── main.py             # FastAPI (GET /chat-ui, POST /api/chat)
│       └── chat_ui.html        # Chat interface (dark theme, iframe-ready)
├── data/
│   ├── raw/                    # Data Lake - Raw (CSV, synced to MinIO)
│   │   ├── klines/{SYMBOL}/{YYYY-MM}.csv
│   │   └── ...
│   └── processed/              # Data Lake - Processed (Parquet, synced to MinIO)
│       └── klines/{SYMBOL}/{YYYY-MM}.parquet
├── models/
│   └── .gitkeep                # Placeholder (reserved for future use)
├── scripts/
│   ├── extract.py              # Data Vision bulk + REST API
│   ├── transform.py            # ClickHouse SQL: RSI-14, MACD-12/26/9 → MinIO processed
│   └── load.py                 # ClickHouse insert (clickhouse-connect + s3)
├── sql/
│   ├── schema.sql              # ClickHouse schema (4 bảng: 1 dim + 3 fact)
│   ├── transform_klines.sql    # ClickHouse SQL transform (RSI + MACD)
│   ├── queries.sql             # Sample ClickHouse queries
│   └── init_db.sql             # Database initialization
├── utils/
│   ├── binance_utils.py        # API wrappers (retry, rate limit)
│   ├── db_utils.py             # ClickHouse client, insert/query helpers
│   ├── data_utils.py           # Timestamps, merge CSV, date utils
│   ├── llm_utils.py            # LLM API callers (Gemini, OpenAI), chat mode
│   ├── storage.py              # MinIO object storage utilities
│   ├── exceptions.py           # Custom exceptions (E/T/L/LLM layers)
│   └── logger.py               # Logging config
├── grafana/
│   ├── provisioning/           # Grafana datasource provisioning
│   └── dashboards/             # Dashboard JSON definitions
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

## 10. Grafana Dashboard

### 10.1. Tổng quan Layout

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                              CRYPTO INTELLIGENCE DASHBOARD                           │
├─────────────────────────────────┬───────────────────────────────────────────────────┤
│                                 │                                                   │
│   TOP 10 VOLUME 24H (Bar)       │           BTC PRICE CHART (Time Series)           │
│   ┌───────────────────────┐     │   ┌─────────────────────────────────────────────┐ │
│   │ BTC  ████████████ 45B │     │   │                    ___/\                    │ │
│   │ ETH  ████████ 32B     │     │   │              /\___/     \                   │ │
│   │ SOL  ██████ 18B       │     │   │         ___/            \___               │ │
│   │ ...                   │     │   │    ____/                                    │ │
│   └───────────────────────┘     │   └─────────────────────────────────────────────┘ │
│                                 │                                                   │
├─────────────────────────────────┼───────────────────────────────────────────────────┤
│                                 │                                                   │
│   TOP LOSERS 24H (Table)        │         AI CHAT ASSISTANT (Iframe Panel)          │
│   ┌───────────────────────┐     │   ┌─────────────────────────────────────────────┐ │
│   │ FTM   -8.3%           │     │   │ User: Tại sao đợt này BTC tăng?             │ │
│   │ ALGO  -6.2%           │     │   │ Bot: Dựa theo 30 nến gần nhất, RSI đang ở...│ │
│   │ MANA  -5.8%           │     │   │                                             │ │
│   └───────────────────────┘     │   └─────────────────────────────────────────────┘ │
│                                 │                                                   │
├─────────────────────────────────┴───────────────────────────────────────────────────┤
│                          RSI HEATMAP (50 COINS)                                     │
│   ┌─────────────────────────────────────────────────────────────────────────────┐   │
│   │  BTC:68  ETH:55  BNB:42  SOL:71  XRP:38  DOGE:62  ADA:45  TRX:58  ...       │   │
│   │  ■ >70 Overbought   ■ 30-70 Neutral   ■ <30 Oversold                        │   │
│   └─────────────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

### 10.2. Chi tiết các Panels

#### Panel 1: Top 10 Volume 24h (Bar Chart)

| Thuộc tính  | Giá trị                |
| ----------- | ---------------------- |
| Loại        | Bar Chart (horizontal) |
| Data source | ClickHouse             |
| Refresh     | 1 giờ                  |

```sql
SELECT
    s.base_asset AS coin,
    t.quote_volume_24h / 1000000000 AS volume_billion_usd
FROM ticker_24h t
JOIN symbols s ON t.symbol = s.symbol
WHERE t.snapshot_time = (SELECT max(snapshot_time) FROM ticker_24h)
ORDER BY t.quote_volume_24h DESC
LIMIT 10;
```

#### Panel 2: Top Gainers 24h (Table)

| Thuộc tính | Giá trị                         |
| ---------- | ------------------------------- |
| Loại       | Table                           |
| Highlight  | Green background for positive % |

```sql
SELECT
    s.base_asset AS coin,
    t.price_change_pct AS change_24h,
    t.quote_volume_24h AS volume_usd
FROM ticker_24h t
JOIN symbols s ON t.symbol = s.symbol
WHERE t.snapshot_time = (SELECT max(snapshot_time) FROM ticker_24h)
  AND t.price_change_pct > 0
ORDER BY t.price_change_pct DESC
LIMIT 5;
```

#### Panel 3: Top Losers 24h (Table)

| Thuộc tính | Giá trị                       |
| ---------- | ----------------------------- |
| Loại       | Table                         |
| Highlight  | Red background for negative % |

```sql
SELECT
    s.base_asset AS coin,
    t.price_change_pct AS change_24h,
    t.quote_volume_24h AS volume_usd
FROM ticker_24h t
JOIN symbols s ON t.symbol = s.symbol
WHERE t.snapshot_time = (SELECT max(snapshot_time) FROM ticker_24h)
  AND t.price_change_pct < 0
ORDER BY t.price_change_pct ASC
LIMIT 5;
```

#### Panel 4: Price Chart (Candlestick)

| Thuộc tính | Giá trị                         |
| ---------- | ------------------------------- |
| Loại       | Candlestick                     |
| Symbol     | Theo biến Grafana `$symbol`     |
| Time range | Theo Grafana global time picker |

> **Lưu ý:** Dữ liệu gốc là nến 1 phút từ `klines`, cập nhật bởi `minutely_etl` DAG. Panel tự aggregate OHLC theo time range người dùng chọn để chart không quá dày khi xem range dài.

**Adaptive candle interval**

| Grafana time range | Candle hiển thị |
| ------------------ | --------------- |
| `<= 6h`            | `1m`            |
| `<= 24h`           | `5m`            |
| `<= 3d`            | `15m`           |
| `<= 7d`            | `1h`            |
| `<= 30d`           | `4h`            |
| `<= 90d`           | `12h`           |
| `<= 365d`          | `1d`            |
| `> 365d`           | `1w`            |

OHLC được tính từ nến 1 phút:

| Field   | Cách aggregate              |
| ------- | --------------------------- |
| `open`  | `argMin(open, timestamp)`   |
| `high`  | `max(high)`                 |
| `low`   | `min(low)`                  |
| `close` | `argMax(close, timestamp)`  |

```sql
WITH
  fromUnixTimestamp64Milli(toInt64(${__from})) AS from_time,
  fromUnixTimestamp64Milli(toInt64(${__to})) AS to_time,
  dateDiff('second', from_time, to_time) AS range_seconds,
  multiIf(
    range_seconds <= 6 * 3600, 60,
    range_seconds <= 24 * 3600, 300,
    range_seconds <= 3 * 24 * 3600, 900,
    range_seconds <= 7 * 24 * 3600, 3600,
    range_seconds <= 30 * 24 * 3600, 14400,
    range_seconds <= 90 * 24 * 3600, 43200,
    range_seconds <= 365 * 24 * 3600, 86400,
    604800
  ) AS bucket_seconds
SELECT
  toStartOfInterval(timestamp, toIntervalSecond(bucket_seconds)) AS time,
  argMin(open, timestamp) AS open,
  max(high) AS high,
  min(low) AS low,
  argMax(close, timestamp) AS close
FROM klines FINAL
WHERE symbol = '${symbol}'
  AND timestamp >= from_time
  AND timestamp <= to_time
GROUP BY time
ORDER BY time;
```

#### Panel 5: AI Chat Assistant (Text/HTML)

| Thuộc tính | Giá trị                                     |
| ---------- | ------------------------------------------- |
| Loại       | Text (với Iframe)                           |
| Nội dung   | `<iframe src="http://localhost:8501/chat-ui?symbol=$symbol" ...></iframe>` |

> **Lưu ý:** Panel này kết nối trực tiếp với backend FastAPI để cung cấp trải nghiệm chat tương tác thay vì hiển thị dữ liệu tĩnh. Đòi hỏi Grafana bật `GF_PANELS_DISABLE_SANITIZE_HTML`.

#### Panel 7: Crypto RSI Heatmap (Scatter)

| Thuộc tính | Giá trị                                                |
| ---------- | ------------------------------------------------------ |
| Loại       | Business Charts / Apache ECharts scatter heatmap       |
| Trục X     | `quote_volume_24h` theo log scale (proxy market size)  |
| Trục Y     | RSI(14), min `10`, max `90`                            |
| Timeframe  | Theo biến `$rsi_tf`: `1h`, `4h`, `1d`                  |
| Filter     | Theo biến `$rsi_zone`: All/Overbought/Strong/Neutral/Weak/Oversold |

> **Lưu ý:** V1 dùng `quote_volume_24h` thay cho market cap để không cần thêm API/ETL ngoài. Nếu cần giống CoinMarketCap/CoinGlass hơn, phase sau có thể thêm bảng market cap từ CoinGecko hoặc CoinMarketCap.

Panel aggregate nến 1 phút thành timeframe đã chọn, rồi tính lại RSI(14) trên close của timeframe đó. Chỉ coin `TRADING` có đủ RSI và volume mới được hiển thị.

| Zone       | Điều kiện RSI |
| ---------- | ------------- |
| Overbought | `RSI >= 70`   |
| Strong     | `60 <= RSI < 70` |
| Neutral    | `40 <= RSI < 60` |
| Weak       | `30 <= RSI < 40` |
| Oversold   | `RSI < 30`    |

ECharts hiển thị:
- nền ngang theo các zone RSI;
- chấm coin đổi màu theo zone;
- label là `base_asset`;
- tooltip gồm RSI, RSI delta, volume 24h, 24h %, latest close;
- đường dashed từ RSI kỳ trước tới RSI hiện tại để thấy momentum.

```sql
WITH
  '$rsi_tf' AS selected_tf,
  '$rsi_zone' AS selected_zone,
  multiIf(selected_tf = '1h', 3600, selected_tf = '4h', 14400, 86400) AS tf_seconds,
  (SELECT max(timestamp) FROM klines FINAL) AS latest_ts,
  latest_ts - toIntervalSecond(tf_seconds * 80) AS from_ts
-- Query chính aggregate klines theo timeframe, tính RSI(14), join ticker_24h,
-- filter status TRADING và filter zone nếu selected_zone != 'All'.
```

### 10.3. Variables (Dropdown filters)

| Variable     | Query                                 | Mục đích                  |
| ------------ | ------------------------------------- | ------------------------- |
| `$symbol`    | `SELECT DISTINCT symbol FROM symbols` | Chọn coin để xem chi tiết |
| `$rsi_tf`    | `1h,4h,1d`                            | Chọn timeframe RSI thật cho heatmap |
| `$rsi_zone`  | `All,Overbought,Strong,Neutral,Weak,Oversold` | Lọc vùng RSI trên heatmap |

### 10.4. Alerts (Cảnh báo)

| Alert                 | Điều kiện                               | Notification |
| --------------------- | --------------------------------------- | ------------ |
| RSI Overbought        | RSI > 70 cho BTC hoặc ETH               | Slack/Email  |
| Volume Spike          | Volume 24h tăng > 200% so với hôm trước | Slack        |

### 10.5. Refresh Rate

| Panel type        | Refresh interval | Giải thích                                       |
| ----------------- | ---------------- | ------------------------------------------------ |
| Price charts      | 1 phút           | Klines cập nhật minutely; candle interval tự đổi theo Grafana time range |
| Volume/Gainers    | 1 phút           | Ticker snapshot cập nhật mỗi phút                 |
| RSI Heatmap       | 1 phút           | Aggregate klines theo `$rsi_tf`, tính RSI(14), cập nhật theo klines/ticker mỗi phút |

---

## 11. Lộ trình Thực hiện (4 Tuần)

### Tuần 1: Setup & Extract

| Ngày | Task                                                      | Deliverable                      |
| ---- | --------------------------------------------------------- | -------------------------------- |
| 1-2  | Setup Docker (ClickHouse, MinIO, Airflow, Grafana)        | docker-compose.yml chạy được     |
| 3-4  | Viết extract.py, download dữ liệu 3 năm                   | MinIO raw/ đầy đủ CSV            |
| 5-7  | Test, verify data integrity                               | Không thiếu nến, không duplicate |

### Tuần 2: Transform & Load

| Ngày | Task                              | Deliverable                     |
| ---- | --------------------------------- | ------------------------------- |
| 1-3  | Viết transform.py với ClickHouse SQL       | MinIO processed/klines/     |
| 4-5  | Viết load.py, tạo bảng ClickHouse | Dữ liệu trong DB, query được    |
| 6-7  | Test full ETL pipeline            | E→T→L chạy end-to-end           |

### Tuần 3: Airflow & Automation

| Ngày | Task                                      | Deliverable                  |
| ---- | ----------------------------------------- | ---------------------------- |
| 1-2  | Setup Airflow, tạo minutely_etl DAG      | DAG chạy được từ UI          |
| 3-4  | Test mini-batch pipeline end-to-end      | Klines + ticker + OB mỗi phút |
| 5-7  | Test scheduling, error handling           | Retry hoạt động, logs đầy đủ |

### Tuần 4: LLM Chat Assistant & Dashboard

| Ngày | Task                                    | Deliverable                    |
| ---- | --------------------------------------- | ------------------------------ |
| 1-3  | Dựng FastAPI chatbot, test LLM Context | API query ClickHouse + LLM chat|
| 4-5  | Setup UI iframe & Grafana dashboard     | Dashboard hiển thị đúng        |
| 6-7  | Test end-to-end, viết báo cáo           | Demo hoàn chỉnh                |

---

## 12. Checklist Trước Khi Demo

### Data Pipeline

- [ ] Extract: Download đủ 50 coins × 3 năm
- [ ] Transform: RSI, MACD tính đúng
- [ ] Load: Dữ liệu trong ClickHouse, query nhanh

### Automation

- [ ] Airflow Web UI accessible (port 8080)
- [ ] minutely_etl DAG chạy thành công (klines + ticker + order book)

### Chat Assistant

- [ ] LLM API connected (Gemini/OpenAI)
- [ ] Backend FastAPI xử lý context thành công
- [ ] Fallback error hiển thị đúng trên chatbox khi hết quota

### Visualization

- [ ] Grafana accessible (port 3000)
- [ ] Dashboard hiển thị chart / tables
- [ ] Chatbox iframe tương tác mượt mà

---

## 13. Các Rủi ro & Giải pháp

| Rủi ro                    | Xác suất   | Tác động        | Giải pháp                                         |
| ------------------------- | ---------- | --------------- | ------------------------------------------------- |
| Binance rate limit        | Cao        | ETL fail        | Dùng Binance Data Vision cho historical data      |
| API lỗi tạm thời          | Trung bình | Mất data ngắn   | Retry với exponential backoff, skip 404 ngay       |
| Thiếu dữ liệu theo phút   | Trung bình | LLM context lỗi | Resample + forward fill hoặc linear interpolation |
| Downtime ngắn (< 30 ngày) | Thấp       | Gap dữ liệu     | Self-healing tự phát hiện và dùng REST API backfill |
| Downtime dài (≥ 30 ngày)  | Thấp       | Gap dữ liệu lớn | Self-healing dùng Data Vision bulk + REST API phần còn lại |
| Coin bị delist/migrate    | Thấp       | Dữ liệu bị cắt  | BREAK status + break_date giới hạn fetch tự động  |
| ClickHouse query timeout   | Trung bình | Transform fail  | Tối ưu SQL, partition theo tháng                   |
| LLM quota exceeded        | Trung bình | Chat fail       | Hiển thị error message thân thiện trên chatbox    |
| LLM response invalid      | Thấp       | Parse error     | Retry 3 lần, báo lỗi format trên giao diện        |
| ClickHouse slow query     | Thấp       | Dashboard lag   | Partition by month, ORDER BY tối ưu                |
| Airflow scheduler crash   | Thấp       | Jobs không chạy | Auto-restart với Docker, monitoring               |
